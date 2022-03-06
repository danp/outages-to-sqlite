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

	ob, err := st.beginObservation()
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	id, err := ob.emit(trackedOutage{
		Outage: outage{
			Desc: outageDesc{
				Cause: "Damage Causing Partial Power",
				CustA: outageDescCustA{Val: 42},
			},
		},
		Events: []trackingEvent{
			{ObservedAt: now, Name: "Initial"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = ob.emit(trackedOutage{
		Outage: outage{
			Desc: outageDesc{
				Cause: "Trees On Line",
				CustA: outageDescCustA{Val: 32},
			},
		},
		Events: []trackingEvent{
			{ObservedAt: now, Name: "Initial"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = ob.emit(trackedOutage{
		ID: id,
		Outage: outage{
			Desc: outageDesc{
				Cause: "Damage Causing Partial Power",
				CustA: outageDescCustA{Val: 42},
			},
		},
		Events: []trackingEvent{
			{ObservedAt: now.Add(time.Minute), Name: "Missing"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := ob.close(); err != nil {
		t.Fatal(err)
	}

	got, err := st.currentOutages()
	if err != nil {
		t.Fatal(err)
	}

	want := map[int]trackedOutage{
		2: {
			ID:     2,
			Events: []trackingEvent{{ObservedAt: now}},
			Outage: outage{
				Desc: outageDesc{
					Cause: "Trees On Line",
					CustA: outageDescCustA{Val: 32},
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

	if err := db.QueryRow("select count(*) from outage_events where outage_id=?", id).Scan(&found); err != nil {
		t.Fatal(err)
	}

	if found != 1 {
		t.Errorf("found %d outage_events with outage_id %d in database", id, found)
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

	if err := db.QueryRow("select count(*) from outage_events where outage_id=?", to.ID).Scan(&found); err != nil {
		t.Fatal(err)
	}

	if found != 2 {
		t.Errorf("found %d outage_events with outage_id %d in database", to.ID, found)
	}
}
