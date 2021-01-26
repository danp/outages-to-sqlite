package main

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "modernc.org/sqlite"
)

func TestStoreCurrentOutages(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	st := &store{db: db}
	if err := st.init(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
insert into outages (id) values (2), (3), (4), (5), (6), (7);
insert into outages_events (outage_id,observed_at,event_name,id,cause,cust_aff,start,etr,geom_p,lon,lat,county) values
(2,'2021-01-18T19:34:30Z','Missing','2','Damage Causing Partial Power',4,'2021-01-18T18:15:00Z','2021-01-18T22:15:00Z','q{nuGb_fyJ',null,null,null),
(3,'2021-01-18T19:34:31Z','Update','2','Damage Causing Partial Power',4,'2021-01-18T18:15:00Z','2021-01-18T22:15:00Z','q{nuGb_fyJ',null,null,null),
(4,'2021-01-18T19:34:32Z','Update','3','Planned Maintenance',7,'2021-01-18T18:15:16Z','2021-01-18T20:00:00Z','{yrqGb_ylK',-63.12565,45.04342,null),
(5,'2021-01-18T19:34:33Z','Initial','1','Under Investigation',4,'2021-01-18T19:22:00Z',NULL,'wchyGv|vmJ',null,null,'Halifax'),
(6,'2021-01-18T19:34:34Z','Missing','1','Under Investigation',4,'2021-01-18T19:22:00Z','2021-01-18T23:15:00Z','wchyGv|vmJ',null,null,null),
(7,'2021-01-18T19:34:35Z','Initial','1',null,4,'2021-01-18T19:22:00Z',NULL,'orluGrcouJ',null,null,'Halifax')
`); err != nil {
		t.Fatal(err)
	}

	got, err := st.currentOutages()
	if err != nil {
		t.Fatal(err)
	}

	want := map[int]trackedOutage{
		3: {
			ID: 3,
			Events: []trackingEvent{
				{
					ObservedAt: time.Date(2021, 1, 18, 19, 34, 31, 0, time.UTC),
					Name:       "Update",
				},
			},
			Outage: outage{
				ID: "2",
				Desc: outageDesc{
					Cause: "Damage Causing Partial Power",
					CustA: outageDescCustA{
						Val: 4,
					},
					ETR:   weirdZoneTime{Time: time.Date(2021, 1, 18, 22, 15, 0, 0, time.UTC)},
					Start: weirdZoneTime{Time: time.Date(2021, 1, 18, 18, 15, 0, 0, time.UTC)},
				},
				Geom: outageGeom{
					P: []string{"q{nuGb_fyJ"},
				},
			},
		},
		4: {
			ID: 4,
			Events: []trackingEvent{
				{
					ObservedAt: time.Date(2021, 1, 18, 19, 34, 32, 0, time.UTC),
					Name:       "Update",
				},
			},
			Outage: outage{
				ID: "3",
				Desc: outageDesc{
					Cause: "Planned Maintenance",
					CustA: outageDescCustA{
						Val: 7,
					},
					ETR:   weirdZoneTime{Time: time.Date(2021, 1, 18, 20, 0, 0, 0, time.UTC)},
					Start: weirdZoneTime{Time: time.Date(2021, 1, 18, 18, 15, 16, 0, time.UTC)},
				},
				Geom: outageGeom{
					P:   []string{"{yrqGb_ylK"},
					Lon: -63.12565,
					Lat: 45.04342,
				},
			},
		},
		5: {
			ID: 5,
			Events: []trackingEvent{
				{
					ObservedAt: time.Date(2021, 1, 18, 19, 34, 33, 0, time.UTC),
					Name:       "Initial",
				},
			},
			Outage: outage{
				ID: "1",
				Desc: outageDesc{
					Cause: "Under Investigation",
					CustA: outageDescCustA{
						Val: 4,
					},
					ETR:   weirdZoneTime{},
					Start: weirdZoneTime{Time: time.Date(2021, 1, 18, 19, 22, 0, 0, time.UTC)},
				},
				Geom: outageGeom{
					P:      []string{"wchyGv|vmJ"},
					County: "Halifax",
				},
			},
		},
		7: {
			ID: 7,
			Events: []trackingEvent{
				{
					ObservedAt: time.Date(2021, 1, 18, 19, 34, 35, 0, time.UTC),
					Name:       "Initial",
				},
			},
			Outage: outage{
				ID: "1",
				Desc: outageDesc{
					Cause: "",
					CustA: outageDescCustA{
						Val: 4,
					},
					ETR:   weirdZoneTime{},
					Start: weirdZoneTime{Time: time.Date(2021, 1, 18, 19, 22, 0, 0, time.UTC)},
				},
				Geom: outageGeom{
					P:      []string{"orluGrcouJ"},
					County: "Halifax",
				},
			},
		},
	}

	if d := cmp.Diff(want, got, cmp.AllowUnexported(trackedOutage{})); d != "" {
		t.Errorf("current outages mismatch (-want +got):\n%s", d)
	}
}

func TestStoreCurrentOutagesEmpty(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	st := &store{db: db}
	if err := st.init(); err != nil {
		t.Fatal(err)
	}

	got, err := st.currentOutages()
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 0 {
		t.Errorf("got current outages\n%+v\nwant empty", got)
	}
}

func TestStoreEmitNew(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	st := &store{db: db}
	if err := st.init(); err != nil {
		t.Fatal(err)
	}

	to := trackedOutage{
		Events: []trackingEvent{
			{
				ObservedAt: time.Date(2021, 1, 18, 19, 34, 33, 0, time.UTC),
				Name:       "Initial",
			},
		},
		Outage: outage{
			ID: "1",
			Desc: outageDesc{
				Cause: "Under Investigation",
				CustA: outageDescCustA{
					Val: 4,
				},
				ETR:   weirdZoneTime{},
				Start: weirdZoneTime{Time: time.Date(2021, 1, 18, 19, 22, 0, 0, time.UTC)},
			},
			Geom: outageGeom{
				P: []string{"wchyGv|vmJ"},
			},
		},
	}

	id, err := st.emit(to)
	if err != nil {
		t.Fatal(err)
	}

	var found int
	if err := db.QueryRow("select count(*) from outages where id=?", id).Scan(&found); err != nil {
		t.Fatal(err)
	}

	if found != 1 {
		t.Errorf("found %d outages with id %d in database", id, found)
	}

	if err := db.QueryRow("select count(*) from outages_events where outage_id=?", id).Scan(&found); err != nil {
		t.Fatal(err)
	}

	if found != 1 {
		t.Errorf("found %d outages_events with outage_id %d in database", id, found)
	}
}

func TestStoreEmitExisting(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	st := &store{db: db}
	if err := st.init(); err != nil {
		t.Fatal(err)
	}

	to := trackedOutage{
		Events: []trackingEvent{
			{
				ObservedAt: time.Date(2021, 1, 18, 19, 34, 33, 0, time.UTC),
				Name:       "Initial",
			},
		},
		Outage: outage{
			ID: "1",
			Desc: outageDesc{
				Cause: "Under Investigation",
				CustA: outageDescCustA{
					Val: 4,
				},
				ETR:   weirdZoneTime{},
				Start: weirdZoneTime{Time: time.Date(2021, 1, 18, 19, 22, 0, 0, time.UTC)},
			},
			Geom: outageGeom{
				P: []string{"wchyGv|vmJ"},
			},
		},
	}

	id, err := st.emit(to)
	if err != nil {
		t.Fatal(err)
	}

	to.ID = id
	to.Events = append(to.Events, trackingEvent{
		ObservedAt: to.Events[0].ObservedAt.Add(10 * time.Minute),
		Name:       "Update",
	})

	id, err = st.emit(to)
	if err != nil {
		t.Fatal(err)
	}

	if id != to.ID {
		t.Errorf("got new id %d after initial id %d", id, to.ID)
	}

	var found int
	if err := db.QueryRow("select count(*) from outages where id=?", to.ID).Scan(&found); err != nil {
		t.Fatal(err)
	}

	if found != 1 {
		t.Errorf("found %d outages with id %d in database", to.ID, found)
	}

	if err := db.QueryRow("select count(*) from outages_events where outage_id=?", to.ID).Scan(&found); err != nil {
		t.Fatal(err)
	}

	if found != 2 {
		t.Errorf("found %d outages_events with outage_id %d in database", to.ID, found)
	}
}
