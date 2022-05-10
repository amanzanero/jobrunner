package jobrunner

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
)

type jobController struct {
	db *sql.DB
}

func setupJobController() (*jobController, error) {
	db, err := sql.Open("sqlite3", "jobs.db")
	if err != nil {
		return nil, err
	}
	return &jobController{db: db}, nil
}

func (j *jobController) createJob() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// write to a database
	}
}

func (j *jobController) deleteJob() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// delete from a database
	}
}

func (j *jobController) updateJob() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// update rows
	}
}

func (j *jobController) getJob() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// grab job
	}
}

func (j *jobController) getJobsList() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// return list of jobs paginated
	}
}

func MakeHttpServer(port string) *http.Server {
	jc, err := setupJobController()
	if err != nil {
		log.Fatalln(err)
	}

	router := http.NewServeMux()
	router.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		create := jc.createJob()
		get := jc.getJob()
		update := jc.updateJob()
		del := jc.deleteJob()

		switch r.Method {
		case http.MethodGet:
			get(w, r)
		case http.MethodPost:
			create(w, r)
		case http.MethodPut:
			update(w, r)
		case http.MethodDelete:
			del(w, r)
		default:
			// no supported
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	srv := http.Server{
		Addr:    port,
		Handler: router,
	}
	return &srv
}
