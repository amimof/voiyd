package delete

import (
	"context"
	"fmt"

	"github.com/amimof/voiyd/pkg/client"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
)

func NewCmdDeleteLease(cfg *client.Config) *cobra.Command {
	runCmd := &cobra.Command{
		Use:     "leases NAME [NAME...]",
		Short:   "Delete one or more leases",
		Long:    "Delete one or more leases",
		Aliases: []string{"lease"},
		Args:    cobra.MinimumNArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tracer := otel.Tracer("voiydctl")
			ctx, span := tracer.Start(ctx, "voiydctl.delete.lease")
			defer span.End()

			// Setup client
			currentSrv, err := cfg.CurrentServer()
			if err != nil {
				logrus.Fatal(err)
			}
			c, err := client.New(currentSrv.Address, client.WithTLSConfigFromCfg(cfg))
			if err != nil {
				logrus.Fatalf("error setting up client: %v", err)
			}
			defer func() {
				if err := c.Close(); err != nil {
					logrus.Errorf("error closing client: %v", err)
				}
			}()

			for _, lname := range args {
				lease, err := c.LeaseV1().Get(ctx, lname)
				if err != nil {
					logrus.Errorf("error getting lease %s: %v", lname, err)
				}
				err = c.LeaseV1().Release(ctx, lease.GetConfig().GetTaskId(), lease.GetConfig().GetNodeId())
				if err != nil {
					logrus.Fatal(err)
				}
				fmt.Printf("Requested to delete lease %s\n", lname)
			}
		},
	}

	return runCmd
}
