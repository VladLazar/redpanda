// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List rpk cloud auths",
		Args:    cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			tw := out.NewTable("name", "kind", "description")
			defer tw.Flush()

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				return
			}

			sort.Slice(y.CloudAuths, func(i, j int) bool {
				return y.CloudAuths[i].Name < y.CloudAuths[j].Name
			})

			var current *string
			if ca := y.Auth(y.CurrentCloudAuth); ca != nil {
				current = &ca.Name
			}
			for i := range y.CloudAuths {
				a := &y.CloudAuths[i]
				name := a.Name
				if current != nil && name == *current {
					name += "*"
				}
				kind, _ := a.Kind()
				tw.Print(name, kind, a.Description)
			}
		},
	}
}
