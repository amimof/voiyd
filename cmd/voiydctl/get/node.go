package get

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/amimof/voiyd/pkg/client"
	"github.com/amimof/voiyd/pkg/cmdutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
)

func NewCmdGetNode(cfg *client.Config) *cobra.Command {
	runCmd := &cobra.Command{
		Use:     "nodes NAME [NAME...] ",
		Short:   "Get one or more nodes",
		Long:    "Get one or more nodes",
		Aliases: []string{"node"},
		Example: `voiydctl get nodes`,
		Args:    cobra.MaximumNArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			tracer := otel.Tracer("voiydctl")
			ctx, span := tracer.Start(ctx, "voiydctl.get.node")
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

			// Setup writer
			wr := tabwriter.NewWriter(os.Stdout, 8, 8, 8, '\t', tabwriter.AlignRight)

			if len(args) == 0 {
				nodes, err := c.NodeV1().List(ctx)
				if err != nil {
					logrus.Fatal(err)
				}
				_, _ = fmt.Fprintf(wr, "%s\t%s\t%s\t%s\t%s\t%s\n", "NAME", "GENERATION", "STATE", "VERSION", "RUNTIME", "AGE")
				for _, n := range nodes {
					_, _ = fmt.Fprintf(wr, "%s\t%d\t%s\t%s\t%s\t%s\n",
						n.GetMeta().GetName(),
						n.GetMeta().GetGeneration(),
						n.GetStatus().GetPhase().GetValue(),
						n.GetStatus().GetVersion().GetValue(),
						n.GetStatus().GetRuntime().GetValue(),
						cmdutil.FormatDuration(time.Since(n.GetMeta().GetCreated().AsTime())),
					)
				}
			}

			_ = wr.Flush()

			if len(args) == 1 {
				name := args[0]
				node, err := c.NodeV1().Get(ctx, name)
				if err != nil {
					logrus.Fatal(err)
				}

				codec, err := cmdutil.CodecFor(output)
				if err != nil {
					logrus.Fatalf("error creating serializer: %v", err)
				}

				b, err := codec.Serialize(node)
				if err != nil {
					logrus.Fatalf("error serializing: %v", err)
				}

				fmt.Printf("%s\n", string(b))
			}
		},
	}

	return runCmd
}
