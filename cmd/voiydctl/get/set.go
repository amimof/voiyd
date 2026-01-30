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

func NewCmdGetContainerSet(cfg *client.Config) *cobra.Command {
	runCmd := &cobra.Command{
		Use:     "sets NAME [NAME...]",
		Short:   "Get one or more containersets",
		Long:    "Get one or more containersets",
		Aliases: []string{"set"},
		Example: `voiydctl get containerset`,
		Args:    cobra.MaximumNArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithTimeout(cmd.Context(), time.Second*30)
			defer cancel()

			tracer := otel.Tracer("voiydctl")
			ctx, span := tracer.Start(ctx, "voiydctl.get.containerset")
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
					logrus.Errorf("error closing client connection: %v", err)
				}
			}()

			// Setup writer
			wr := tabwriter.NewWriter(os.Stdout, 8, 8, 8, '\t', tabwriter.AlignRight)

			if len(args) == 0 {
				containers, err := c.ContainerSetV1().List(ctx)
				if err != nil {
					logrus.Fatal(err)
				}
				_, _ = fmt.Fprintf(wr, "%s\t%s\t%s\n", "NAME", "GENERATION", "AGE")
				for _, c := range containers {
					_, _ = fmt.Fprintf(wr, "%s\t%d\t%s\n",
						c.GetMeta().GetName(),
						c.GetMeta().GetGeneration(),
						cmdutil.FormatDuration(time.Since(c.GetMeta().GetCreated().AsTime())),
					)
				}
			}

			_ = wr.Flush()

			if len(args) == 1 {
				cname := args[0]
				container, err := c.ContainerSetV1().Get(ctx, cname)
				if err != nil {
					logrus.Fatal(err)
				}

				codec, err := cmdutil.CodecFor(output)
				if err != nil {
					logrus.Fatalf("error creating serializer: %v", err)
				}

				b, err := codec.Serialize(container)
				if err != nil {
					logrus.Fatalf("error serializing: %v", err)
				}

				fmt.Printf("%s\n", string(b))
			}
		},
	}

	return runCmd
}
