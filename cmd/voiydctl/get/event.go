package get

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"

	"github.com/amimof/voiyd/pkg/client"
	"github.com/amimof/voiyd/pkg/cmdutil"

	eventsv1 "github.com/amimof/voiyd/api/services/events/v1"
)

func NewCmdGetEvent(cfg *client.Config) *cobra.Command {
	runCmd := &cobra.Command{
		Use:     "events NAME [NAME...]",
		Short:   "Get one or more events",
		Long:    "Get one or more events",
		Aliases: []string{"event"},
		Example: `voiydctl get events`,
		Args:    cobra.MaximumNArgs(2),
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
			ctx, span := tracer.Start(ctx, "voiydctl.get.event")
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

			// List all events
			if len(args) == 0 {

				events, err := c.EventV1().List(ctx)
				if err != nil {
					logrus.Fatal(err)
				}
				// Setup writer
				wr := tabwriter.NewWriter(os.Stdout, 8, 8, 8, '\t', tabwriter.AlignRight)

				_, _ = fmt.Fprintf(wr, "%s\t%s\t%s\t%s\t%s\n", "ID", "RESOURCE", "GENEREATION", "TYPE", "AGE")
				for _, event := range events {
					ver, _ := extractVersionFromAny(event)
					objName := event.GetMeta().GetLabels()["voiyd.io/object-id"]
					_, _ = fmt.Fprintf(wr, "%s\t%s\t%s\t%s\t%s\n",
						event.GetMeta().GetName(),
						objName,
						ver,
						event.GetType().String(),
						cmdutil.FormatDuration(time.Since(event.GetMeta().GetCreated().AsTime())),
					)
				}
				_ = wr.Flush()
			}

			// List one event
			if len(args) > 0 {
				ename := args[0]
				task, err := c.EventV1().Get(ctx, ename)
				if err != nil {
					logrus.Fatal(err)
				}

				codec, err := cmdutil.CodecFor(output)
				if err != nil {
					logrus.Fatalf("error creating serializer: %v", err)
				}

				b, err := codec.Serialize(task)
				if err != nil {
					logrus.Fatalf("error serializing: %v", err)
				}

				fmt.Printf("%s\n", string(b))
			}
		},
	}

	return runCmd
}

type versioned interface {
	GetVersion() string
}

func extractVersionFromAny(o *eventsv1.Event) (string, error) {
	a := o.GetObject()

	if a == nil {
		return "", fmt.Errorf("any is nil")
	}
	msg, err := a.UnmarshalNew()
	if err != nil {
		return "", err
	}
	v, ok := msg.(versioned)
	if !ok {
		return "", fmt.Errorf("message %T does not have GetVersion", msg)
	}
	return v.GetVersion(), nil
}
