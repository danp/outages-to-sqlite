package main

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
	"github.com/peterbourgon/ff"
	"github.com/twpayne/go-polyline"
	_ "modernc.org/sqlite"
)

func main() {
	fs := flag.NewFlagSet("nsp-outage-sync", flag.ExitOnError)
	var (
		databaseFile = fs.String("database-file", "outages.db", "data file path")
		repoRemote   = fs.String("repo-remote", "https://github.com/danp/nspoweroutages.git", "git remote of nspoweroutages repo")
		repoPath     = fs.String("repo-path", "", "path to nspoweroutages git repo clone, preferred over -repo-remote if set")
		placesFile   = fs.String("places-file", "", "featurecollection geojson file to use for turning outage geometries into places, defaults to embedded data")
	)
	ff.Parse(fs, os.Args[1:])

	var openRepo func() (*git.Repository, error)
	if *repoPath != "" {
		openRepo = localOpenRepo(*repoPath)
	} else if *repoRemote != "" {
		openRepo = remoteOpenRepo(*repoRemote)
	} else {
		log.Fatal("need -repo-remote or -repo-path")
	}

	db, err := sql.Open("sqlite", *databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	st := &store{db: db}
	if err := st.init(); err != nil {
		log.Fatal(err)
	}

	places, err := loadPlaces(*placesFile)
	if err != nil {
		log.Fatal(err)
	}

	pl := newPlacer(places)

	tracker := newOutageTracker(st)
	if err := tracker.loadState(); err != nil {
		log.Fatal(err)
	}

	var maxObservedAtS sql.NullString
	if err := db.QueryRow("select max(observed_at) from outages_events").Scan(&maxObservedAtS); err != nil {
		log.Fatal(err)
	}

	var maxObservedAt time.Time
	if maxObservedAtS.Valid {
		moa, err := time.Parse(time.RFC3339, maxObservedAtS.String)
		if err != nil {
			log.Fatal(err)
		}
		maxObservedAt = moa
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
		return err
	}

	head, err := repo.Head()
	if err != nil {
		return err
	}

	logOpts := &git.LogOptions{
		Order: git.LogOrderCommitterTime,
		From:  head.Hash(),
	}
	if !since.IsZero() {
		logOpts.Since = &since
	}

	iter, err := repo.Log(logOpts)
	if err != nil {
		return err
	}

	var commits []*object.Commit
	if err := iter.ForEach(func(c *object.Commit) error {
		commits = append(commits, c)
		return nil
	}); err != nil {
		return err
	}

	var lastHash plumbing.Hash
	process := func(c *object.Commit) error {
		tr, err := c.Tree()
		if err != nil {
			return err
		}
		f, err := tr.File(outagesFileName)
		if err != nil {
			if err == object.ErrFileNotFound {
				return nil
			}
			return err
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
			return err
		}
		defer r.Close()

		return consume(c.Committer.When, r)
	}

	for i := len(commits) - 1; i >= 0; i-- {
		c := commits[i]
		if err := process(c); err != nil {
			return err
		}
	}

	return nil
}

type store struct {
	db *sql.DB
}

func (s *store) init() error {
	if _, err := s.db.Exec("pragma foreign_keys = on"); err != nil {
		return err
	}

	if _, err := s.db.Exec("create table if not exists outages (id integer primary key)"); err != nil {
		return err
	}

	if _, err := s.db.Exec("create table if not exists outages_events (outage_id integer references outages on delete cascade, observed_at datetime, event_name text, id text, cause text, cust_aff integer, start datetime, etr datetime, geom_p text, lon numeric, lat numeric, county text, primary key(outage_id, observed_at))"); err != nil {
		return err
	}

	return nil
}

func (s *store) currentOutages() (map[int]trackedOutage, error) {
	rows, err := s.db.Query(`
with max_observed_ats as (select outage_id, max(observed_at) as max_observed_at from outages_events group by outage_id)
select outages_events.outage_id, observed_at, event_name, id, cause, cust_aff, start, etr,
geom_p, lon, lat, county
from outages_events, max_observed_ats
where max_observed_ats.outage_id=outages_events.outage_id and
max_observed_ats.max_observed_at=outages_events.observed_at and
event_name in ('Initial', 'Update')
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
		var observedAtS string
		var lon, lat sql.NullFloat64
		var cause, gp, county sql.NullString
		if err := rows.Scan(&to.ID, &observedAtS, &ev.Name, &ou.ID, &cause, &ou.Desc.CustA.Val, &ou.Desc.Start, &ou.Desc.ETR, &gp, &lon, &lat, &county); err != nil {
			return nil, err
		}

		observedAt, err := time.Parse(time.RFC3339, observedAtS)
		if err != nil {
			return nil, err
		}
		ev.ObservedAt = observedAt

		ou.Desc.Cause = cause.String

		if gp.Valid && gp.String != "" {
			ou.Geom.P = []string{gp.String}
		}
		ou.Geom.Lon = lon.Float64
		ou.Geom.Lat = lat.Float64
		ou.Geom.County = county.String

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
	Exec(string, ...interface{}) (sql.Result, error)
}, to trackedOutage) (int, error) {
	if to.ID == 0 {
		res, err := execer.Exec("insert into outages (id) values (null)")
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
	var county, cause *string
	if c := to.Outage.Geom.County; c != "" {
		county = &c
	}
	if to.Outage.Desc.Cause != "" {
		cause = &to.Outage.Desc.Cause
	}
	var lon, lat *float64
	if l := to.Outage.Geom.Lat; l != 0 {
		lat = &l
	}
	if l := to.Outage.Geom.Lon; l != 0 {
		lon = &l
	}

	_, err := execer.Exec(
		"insert into outages_events (outage_id, observed_at, event_name, id, cause, cust_aff, start, etr, geom_p, lon, lat, county) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
		to.ID, le.ObservedAt.Format(time.RFC3339), le.Name, to.Outage.ID, cause, to.Outage.Desc.CustA.Val,
		to.Outage.Desc.Start, to.Outage.Desc.ETR,
		to.Outage.Geom.P[0], lon, lat, county,
	)
	return to.ID, err
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

func (w *weirdZoneTime) Scan(value interface{}) error {
	if value == nil {
		w.Time = time.Time{}
		return nil
	}

	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("value %+v is not string for weirdZoneTime.Scan", value)
	}

	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
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
	// maybe later
	// A        []string
	P        []string
	Lon, Lat float64
	County   string
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

func (t trackedOutage) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(b)
}

type outageTracker struct {
	st outageStore
	// geom.p is key
	known map[string]trackedOutage
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
	return &outageTracker{st: st, known: make(map[string]trackedOutage)}
}

func (o *outageTracker) loadState() error {
	co, err := o.st.currentOutages()
	if err != nil {
		return err
	}

	for _, to := range co {
		o.known[to.Outage.Geom.P[0]] = to
	}

	return nil
}

func (o *outageTracker) observe(t time.Time, outages []outage) error {
	log.Println("tracker.observe time", t.Format(time.RFC3339), "knowing", len(o.known), "and observing", len(outages), "outages")

	so, err := o.st.beginObservation()
	if err != nil {
		return err
	}
	defer so.close()

	ui := make(map[string]bool)
nextOutage:
	for _, out := range outages {
		k, ok := o.known[out.Geom.P[0]]
		if ok {
			k.Events = append(k.Events, trackingEvent{ObservedAt: t, Name: "Update"})
			k.Outage = out
			o.known[out.Geom.P[0]] = k
			if _, err := so.emit(k); err != nil {
				return err
			}
			ui[out.Geom.P[0]] = true
			continue nextOutage
		}

		to := trackedOutage{Events: []trackingEvent{{ObservedAt: t, Name: "Initial"}}, Outage: out}
		id, err := so.emit(to)
		if err != nil {
			return err
		}
		to.ID = id
		o.known[out.Geom.P[0]] = to
		ui[out.Geom.P[0]] = true
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
		for _, f := range feats {
			name := f.Properties.MustString("wof:name")
			placeType := f.Properties.MustString("wof:placetype")
			switch placeType {
			case "county":
				out.Geom.County = name
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
