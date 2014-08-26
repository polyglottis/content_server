package server

import (
	"github.com/polyglottis/platform/content"
)

func (s *Server) ExtractsMatching(q *content.Query) ([]content.ExtractId, error) {
	var extracts []*content.Extract
	var err error
	switch {
	case q.LanguageA == "" && q.LanguageB == "":
		extracts, err = s.ExtractList()
	case q.LanguageA != "" && q.LanguageB != "":
		extracts, err = s.ExtractListWithLanguages(q.LanguageA, q.LanguageB)
	default: // exactly one of q.LanguageA or q.LanguageB is non-empty
		extracts, err = s.ExtractListWithLanguage(q.LanguageA + q.LanguageB)
	}
	if err != nil {
		return nil, err
	}
	ids := make([]content.ExtractId, 0, len(extracts))
	if q.ExtractType == "" {
		for _, e := range extracts {
			ids = append(ids, e.Id)
		}
	} else {
		for _, e := range extracts {
			if e.Type == q.ExtractType {
				ids = append(ids, e.Id)
			}
		}
	}
	return ids, nil
}
