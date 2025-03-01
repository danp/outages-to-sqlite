package main

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
	"github.com/peterbourgon/ff"
	"github.com/twpayne/go-polyline"
)

func main() {
	var databaseFile, repoRemote, repoPath, placesFile string
	fs := flag.NewFlagSet("outages-to-sqlite", flag.ExitOnError)
	fs.StringVar(&databaseFile, "database-file", "outages.db", "data file path")
	fs.StringVar(&repoRemote, "repo-remote", "https://github.com/danp/nspoweroutages.git", "git remote of nspoweroutages repo")
	fs.StringVar(&repoPath, "repo-path", "", "path to nspoweroutages git repo clone, preferred over -repo-remote if set")
	fs.StringVar(&placesFile, "places-file", "", "featurecollection geojson file to use for turning outage geometries into places, defaults to embedded data")
	ff.Parse(fs, os.Args[1:])

	var openRepo func() (*git.Repository, error)
	if repoPath != "" {
		openRepo = localOpenRepo(repoPath)
	} else if repoRemote != "" {
		openRepo = remoteOpenRepo(repoRemote)
	} else {
		log.Fatal("need -repo-remote or -repo-path")
	}

	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	st := &store{db: db}
	if err := st.init(); err != nil {
		log.Fatal(err)
	}

	places, err := loadPlaces(placesFile)
	if err != nil {
		log.Fatal(err)
	}

	pl := newPlacer(places)

	tracker := newOutageTracker(st)
	if err := tracker.loadState(); err != nil {
		log.Fatal(err)
	}

	var maxObservedAt time.Time
	if err := db.QueryRow("select max(last_observed) from outage_summaries").Scan(newTimeScanner(&maxObservedAt)); err != nil {
		log.Fatal(err)
	}

	log.Println("tracker starting with", len(tracker.known), "known outages and sourcing after", maxObservedAt)

	consume := func(t time.Time, r io.Reader) error {
		var outages []outage
		if err := json.NewDecoder(r).Decode(&outages); err != nil {
			return fmt.Errorf("decoding outages: %w", err)
		}

		if err := pl.place(outages); err != nil {
			return fmt.Errorf("placing outages: %w", err)
		}

		return tracker.observe(t, outages)
	}

	if err := gitSource(openRepo, "data/outages.json", maxObservedAt, consume); err != nil {
		log.Fatal(err)
	}
}

func localOpenRepo(path string) func() (*git.Repository, error) {
	return func() (*git.Repository, error) {
		log.Println("gitSource using local path", path)
		return git.PlainOpen(path)
	}
}

func remoteOpenRepo(remote string) func() (*git.Repository, error) {
	return func() (*git.Repository, error) {
		log.Println("gitSource cloning from", remote)
		return git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
			URL: remote,
		})
	}
}

func gitSource(openRepo func() (*git.Repository, error), outagesFileName string, since time.Time, consume func(time.Time, io.Reader) error) error {
	repo, err := openRepo()
	if err != nil {
		return fmt.Errorf("opening repo: %w", err)
	}

	head, err := repo.Head()
	if err != nil {
		return fmt.Errorf("head: %w", err)
	}

	logOpts := &git.LogOptions{
		Order: git.LogOrderCommitterTime,
		From:  head.Hash(),
	}
	if !since.IsZero() {
		logSince := since.Add(-time.Second) // we want to see it
		logOpts.Since = &logSince
	}

	iter, err := repo.Log(logOpts)
	if err != nil {
		return fmt.Errorf("log: %w", err)
	}

	var sawSince bool
	var commits []*object.Commit
	for {
		c, err := iter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("iterating commits: %w", err)
		}
		if !since.IsZero() && c.Committer.When.Equal(since) {
			sawSince = true
			break
		}
		commits = append(commits, c)
	}
	if !sawSince {
		return fmt.Errorf("did not see since commit %v", since)
	}

	var lastHash plumbing.Hash
	process := func(c *object.Commit) error {
		tr, err := c.Tree()
		if err != nil {
			return fmt.Errorf("tree: %w", err)
		}
		f, err := tr.File(outagesFileName)
		if err != nil {
			if err == object.ErrFileNotFound {
				return nil
			}
			return fmt.Errorf("file: %w", err)
		}

		// LogOptions.Since includes commit(s) with its value
		// and we want everything after it.
		if !c.Committer.When.After(since) {
			// Still need to update lastHash so real change detection
			// below works.
			copy(lastHash[:], f.Hash[:])
			return nil
		}

		// LogOptions.PathFilter should work here but it is very slow
		// since it does a full diff of the current and parent trees.
		if bytes.Equal(f.Hash[:], lastHash[:]) {
			return nil
		}
		copy(lastHash[:], f.Hash[:])

		r, err := f.Reader()
		if err != nil {
			return fmt.Errorf("reader: %w", err)
		}
		defer r.Close()

		return consume(c.Committer.When, r)
	}

	for i := len(commits) - 1; i >= 0; i-- {
		c := commits[i]
		if err := process(c); err != nil {
			return fmt.Errorf("process: %w", err)
		}
	}

	return nil
}

type store struct {
	db *sql.DB
}

func (s *store) init() error {
	if _, err := s.db.Exec("create table if not exists outages (id integer primary key, longitude numeric, latitude numeric, county text, neighborhood text, area_polyline text)"); err != nil {
		return err
	}

	if _, err := s.db.Exec("create table if not exists outage_events (outage_id integer references outages on delete cascade, observed_at datetime, removed bool, cause text, cust_aff int, start datetime, etr datetime, primary key(outage_id, observed_at))"); err != nil {
		return err
	}

	if _, err := s.db.Exec("create table if not exists outage_summaries (id integer primary key references outages on delete cascade, resolved bool, first_observed datetime, last_observed datetime, observations int, min_cust_aff int, max_cust_aff int, min_start datetime, max_etr datetime, last_cause text, longitude numeric, latitude numeric, county text, neighborhood text)"); err != nil {
		return err
	}

	if _, err := s.db.Exec("create index if not exists outage_summaries_unresolved on outage_summaries (id, last_observed) where resolved=0"); err != nil {
		return err
	}

	return nil
}

func (s *store) currentOutages() (map[int]trackedOutage, error) {
	rows, err := s.db.Query(`
with max_observed_ats as (select id as outage_id, last_observed as max_observed_at from outage_summaries where resolved=0)
select id, longitude, latitude, county, neighborhood, observed_at, cause, cust_aff, start, etr
from outages, outage_events, max_observed_ats
where max_observed_ats.outage_id=outage_events.outage_id and
max_observed_ats.max_observed_at=outage_events.observed_at and
outages.id=outage_events.outage_id
order by observed_at
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int]trackedOutage)
	for rows.Next() {
		var to trackedOutage
		var ev trackingEvent
		var ou outage
		var lon, lat float64
		var cause, county, neighborhood sql.NullString
		if err := rows.Scan(&to.ID, &lon, &lat, &county, &neighborhood, &ev.ObservedAt, &cause, &ou.Desc.CustA.Val, &ou.Desc.Start, &ou.Desc.ETR); err != nil {
			return nil, err
		}

		ou.Desc.Cause = cause.String

		ou.Geom.Lon = lon
		ou.Geom.Lat = lat
		ou.Geom.County = county.String
		ou.Geom.Neighborhood = neighborhood.String

		to.Outage = ou
		to.Events = []trackingEvent{ev}
		out[to.ID] = to
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, rows.Close()
}

type storeObs struct {
	tx *sql.Tx
}

func (s storeObs) emit(to trackedOutage) (int, error) {
	return storeEmitExec(s.tx, to)
}

func (s storeObs) close() error {
	return s.tx.Commit()
}

func (s *store) beginObservation() (storeObservation, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, nil
	}

	return storeObs{tx: tx}, nil
}

func (s *store) emit(to trackedOutage) (int, error) {
	return storeEmitExec(s.db, to)
}

func storeEmitExec(execer interface {
	Exec(string, ...any) (sql.Result, error)
}, to trackedOutage) (int, error) {
	if to.ID == 0 {
		var county, neighborhood, area *string
		if c := to.Outage.Geom.County; c != "" {
			county = &c
		}
		if n := to.Outage.Geom.Neighborhood; n != "" {
			neighborhood = &n
		}
		if len(to.Outage.Geom.A) > 0 {
			area = &to.Outage.Geom.A[0]
		}

		res, err := execer.Exec("insert into outages (longitude, latitude, county, neighborhood, area_polyline) values (?, ?, ?, ?, ?)", to.Outage.Geom.Lon, to.Outage.Geom.Lat, county, neighborhood, area)
		if err != nil {
			return 0, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return 0, err
		}
		to.ID = int(id)
	}

	le := to.Events[len(to.Events)-1]
	removed := le.Name == "Missing"

	var cause *string
	if to.Outage.Desc.Cause != "" {
		cause = &to.Outage.Desc.Cause
	}

	_, err := execer.Exec(
		"insert into outage_events (outage_id, observed_at, removed, cause, cust_aff, start, etr) values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
		to.ID, le.ObservedAt.Format(time.RFC3339), removed, cause, to.Outage.Desc.CustA.Val, to.Outage.Desc.Start, to.Outage.Desc.ETR,
	)
	if err != nil {
		return 0, err
	}

	if _, err := execer.Exec("delete from outage_summaries where id=?", to.ID); err != nil {
		return 0, err
	}

	q := `
with
summary as (
  select
    outage_id as id,
    min(observed_at) as first_observed,
    max(observed_at) as last_observed,
    count(*) as observations,
    min(cust_aff) as min_cust_aff,
    max(cust_aff) as max_cust_aff,
    min(start) as min_start,
    max(etr) as max_etr
  from outage_events
  group by 1
)
insert into outage_summaries
select
summary.id,
(select removed from outage_events where outage_id=summary.id and observed_at=last_observed),
first_observed, last_observed, observations, min_cust_aff, max_cust_aff, min_start, max_etr,
(select cause from outage_events where outage_id=summary.id and observed_at=last_observed),
longitude, latitude, county, neighborhood
from summary, outages
where summary.id=? and summary.id=outages.id
`

	if _, err := execer.Exec(q, to.ID); err != nil {
		return 0, err
	}

	return to.ID, nil
}

type weirdZoneTime struct{ time.Time }

func (w weirdZoneTime) MarshalJSON() ([]byte, error) {
	v, _ := w.Value()
	if v == nil {
		return []byte("null"), nil
	}
	return json.Marshal(v)
}

func (w *weirdZoneTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "\"\"" {
		return nil
	}

	t, err := time.Parse("\"2006-01-02T15:04:05Z0700\"", s)
	if err != nil {
		return err
	}
	*w = weirdZoneTime{t.UTC()}
	return nil
}

func (w weirdZoneTime) Value() (driver.Value, error) {
	if w.Time.IsZero() {
		return nil, nil
	}
	return w.Time.Format(time.RFC3339), nil
}

func (w *weirdZoneTime) Scan(value any) error {
	var t time.Time

	if value == nil {
		w.Time = t
		return nil
	}

	switch x := value.(type) {
	case string:
		tt, err := time.Parse(time.RFC3339, x)
		if err != nil {
			return err
		}
		t = tt
	case time.Time:
		t = x
	default:
		return fmt.Errorf("value %+v is not acceptable for weirdZoneTime.Scan", value)
	}

	w.Time = t
	return nil
}

type outageDescCustA struct {
	Masked bool
	Val    int
}

type outageDesc struct {
	Cause   string
	Cluster bool
	CustA   outageDescCustA `json:"cust_a"`
	NOut    int             `json:"n_out"`
	Outages []outageDesc
	ETR     weirdZoneTime
	Start   weirdZoneTime
}

type outageGeom struct {
	A            []string
	P            []string
	Lon, Lat     float64
	County       string
	Neighborhood string
}

type outage struct {
	Desc  outageDesc
	Geom  outageGeom
	ID    string
	Title string
}

type trackingEvent struct {
	ObservedAt time.Time
	Name       string
}

type trackedOutage struct {
	ID     int
	Events []trackingEvent
	Outage outage
}

type outageTracker struct {
	st outageStore
	// geom.Lon/Lat is key
	known map[lonLatKey]trackedOutage
}

type outageStore interface {
	currentOutages() (map[int]trackedOutage, error)
	beginObservation() (storeObservation, error)
	emit(trackedOutage) (int, error)
}

type storeObservation interface {
	emit(trackedOutage) (int, error)
	close() error
}

func newOutageTracker(st outageStore) *outageTracker {
	return &outageTracker{st: st, known: make(map[lonLatKey]trackedOutage)}
}

func (o *outageTracker) loadState() error {
	co, err := o.st.currentOutages()
	if err != nil {
		return err
	}

	for _, to := range co {
		o.known[lonLatKey{to.Outage.Geom.Lon, to.Outage.Geom.Lat}] = to
	}

	return nil
}

type lonLatKey struct {
	lon, lat float64
}

func (o *outageTracker) observe(t time.Time, outages []outage) error {
	log.Println("tracker.observe time", t.Format(time.RFC3339), "knowing", len(o.known), "and observing", len(outages), "outages")

	so, err := o.st.beginObservation()
	if err != nil {
		return err
	}
	defer so.close()

	ui := make(map[lonLatKey]bool)
nextOutage:
	for _, out := range outages {
		key := lonLatKey{out.Geom.Lon, out.Geom.Lat}
		k, ok := o.known[key]
		if ok {
			k.Events = append(k.Events, trackingEvent{ObservedAt: t, Name: "Update"})
			k.Outage = out
			o.known[key] = k
			if _, err := so.emit(k); err != nil {
				return err
			}
			ui[key] = true
			continue nextOutage
		}

		to := trackedOutage{Events: []trackingEvent{{ObservedAt: t, Name: "Initial"}}, Outage: out}
		id, err := so.emit(to)
		if err != nil {
			return err
		}
		to.ID = id
		o.known[key] = to
		ui[key] = true
	}

	for ki, ko := range o.known {
		if !ui[ki] {
			ko := ko
			ko.Events = append(ko.Events, trackingEvent{ObservedAt: t, Name: "Missing"})
			if _, err := so.emit(ko); err != nil {
				return err
			}
			delete(o.known, ki)
		}
	}

	return so.close()
}

//go:embed places/ns-featurecollection.json
var defaultPlaceData []byte

func loadPlaces(path string) (*geojson.FeatureCollection, error) {
	var placeData []byte
	if path != "" {
		pd, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		placeData = pd
	} else {
		placeData = defaultPlaceData
	}

	fc, err := geojson.UnmarshalFeatureCollection(placeData)
	if err != nil {
		return nil, err
	}

	return fc, nil
}

type placer struct {
	places      *geojson.FeatureCollection
	ptFeatCache map[orb.Point][]*geojson.Feature
}

func newPlacer(places *geojson.FeatureCollection) *placer {
	return &placer{places: places, ptFeatCache: make(map[orb.Point][]*geojson.Feature)}
}

func (p *placer) place(outages []outage) error {
	for i, out := range outages {
		ps := out.Geom.P
		if len(ps) == 0 {
			continue
		}

		coord, _, err := polyline.DecodeCoord([]byte(ps[0]))
		if err != nil {
			return fmt.Errorf("decoding geom.p %q: %w", ps[0], err)
		}

		out.Geom.Lon, out.Geom.Lat = coord[1], coord[0]

		pt := orb.Point{coord[1], coord[0]}
		feats, ok := p.ptFeatCache[pt]
		if !ok {
			for _, f := range p.places.Features {
				if isPointWithinFeature(pt, f) {
					feats = append(feats, f)
				}
			}
			p.ptFeatCache[pt] = feats
		}
		var neighborhoodArea float64
		for _, f := range feats {
			name := f.Properties.MustString("wof:name")
			placeType := f.Properties.MustString("wof:placetype")

			switch placeType {
			case "county":
				out.Geom.County = name
			case "neighbourhood": // whosonfirst spelling
				// Prefer smallest neighborhood match.
				if fa := planar.Area(f.Geometry); neighborhoodArea == 0 || fa < neighborhoodArea {
					out.Geom.Neighborhood = name
					neighborhoodArea = fa
				}
			}
		}

		outages[i] = out
	}

	return nil
}

// https://golangcode.com/is-point-within-polygon-from-geojson/
//
// isPointWithinFeature returns whether point is contained
// by feat.
func isPointWithinFeature(point orb.Point, feat *geojson.Feature) bool {
	if op, ok := feat.Geometry.(orb.Point); ok {
		return op.Equal(point)
	}

	if pg, ok := feat.Geometry.(orb.Polygon); ok {
		return planar.PolygonContains(pg, point)
	}

	if mp, ok := feat.Geometry.(orb.MultiPolygon); ok {
		return planar.MultiPolygonContains(mp, point)
	}

	return false
}

// timeScanner helps when scanning results of expressions
// (eg `max(observed_at)` where observed_at is datetime)
// which return datetime values that should be scanned to
// time.Time.
//
// sqlite doesn't report that the result of the expression
// is also datetime so its type is returned as text/string.
//
// TODO: mostly copied from modernc.org/sqlite, move to sqlite helper?
type timeScanner struct {
	dest *time.Time
}

func newTimeScanner(dest *time.Time) timeScanner {
	return timeScanner{dest: dest}
}

var parseTimeFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

func (s timeScanner) Scan(value any) error {
	if value == nil {
		*s.dest = time.Time{}
		return nil
	}

	if vt, ok := value.(time.Time); ok {
		*s.dest = vt
		return nil
	}

	vs, ok := value.(string)
	if !ok {
		return fmt.Errorf("unknown timeScanner.Scan type %T", value)
	}

	if vt, ok := s.parseTimeString(vs); ok {
		*s.dest = vt
		return nil
	}

	ts := strings.TrimSuffix(vs, "Z")

	for _, f := range parseTimeFormats {
		t, err := time.Parse(f, ts)
		if err == nil {
			*s.dest = t
			return nil
		}
	}

	return fmt.Errorf("could not parse time string %q", vs)
}

func (s timeScanner) parseTimeString(v string) (time.Time, bool) {
	meq := strings.Index(v, "m=")
	if meq < 1 {
		return time.Time{}, false
	}

	v = v[:meq-1] // "2006-01-02 15:04:05.999999999 -0700 MST m=+9999" -> "2006-01-02 15:04:05.999999999 -0700 MST "
	v = strings.TrimSpace(v)
	t, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", v)
	return t, err == nil
}
