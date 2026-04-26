package agent

import (
	"testing"
)

func TestMarketingAgencyPackExists(t *testing.T) {
	found := LookupLegacyPack("marketing-agency")
	if found == nil {
		t.Fatal("marketing-agency pack not found in legacyPacks")
	}
	if found.LeadSlug == "" {
		t.Error("marketing-agency pack must have a LeadSlug")
	}

	slugs := make(map[string]bool)
	for _, a := range found.Agents {
		if slugs[a.Slug] {
			t.Errorf("duplicate agent slug: %s", a.Slug)
		}
		slugs[a.Slug] = true
	}

	required := []string{"director", "content-lead", "copywriter", "seo-analyst"}
	for _, s := range required {
		if !slugs[s] {
			t.Errorf("marketing-agency pack missing required agent: %s", s)
		}
	}
}
