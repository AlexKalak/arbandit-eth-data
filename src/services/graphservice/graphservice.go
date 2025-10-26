package graphservice

type GraphService interface {
	CheckToken() error
}

type graphService struct {
}

var singleton *graphService

func New() (*graphService, error) {
	if singleton != nil {
		return singleton, nil
	}

	singleton = &graphService{}

	return singleton, nil
}
