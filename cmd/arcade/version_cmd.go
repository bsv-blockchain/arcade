package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/bsv-blockchain/arcade/version"
)

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the build version and embedded source revision",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			info, err := version.Read()
			if err != nil {
				return err
			}
			fmt.Fprint(cmd.OutOrStdout(), info.Format())
			return nil
		},
	}
}
