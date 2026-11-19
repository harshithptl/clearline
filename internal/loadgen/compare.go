package loadgen

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// Configuration is one labeled report in a comparison chart.
type Configuration struct {
	Name   string
	Report Report
}

var compareColors = []string{"#6b6b66", "#d1495b", "#2f6fdb", "#2a9d6b", "#e08a1e"}

// PlotCompare writes one line per configuration: order-to-ledger latency at
// the chosen quantile (y, log scale) against settled trades per second (x).
// Hollow points were not sustained. It shows how far each change moved the
// curve to the right (more throughput) and down (less latency).
func PlotCompare(w io.Writer, title, subtitle string, quantile string, configs []Configuration) error {
	pick := func(r Result) time.Duration {
		if quantile == "p50" {
			return r.Pipeline.OrderToLedger.P50
		}
		return r.Pipeline.OrderToLedger.P99
	}
	const (
		width, height          = 760, 440
		left, right, top, bott = 70, 210, 62, 56
	)
	pw, ph := float64(width-left-right), float64(height-top-bott)
	maxX, minY, maxY := 0.0, math.Inf(1), 0.0
	for _, c := range configs {
		for _, r := range c.Report.Runs {
			maxX = math.Max(maxX, r.SettledPerSec)
			if v := msf(pick(r)); v > 0 {
				minY, maxY = math.Min(minY, v), math.Max(maxY, v)
			}
		}
	}
	if maxX == 0 || maxY == 0 {
		return fmt.Errorf("nothing to compare")
	}
	maxX = niceCeil(maxX * 1.05)
	lo, hi := math.Floor(math.Log10(minY)), math.Ceil(math.Log10(maxY))
	if hi == lo {
		hi = lo + 1
	}
	xpos := func(v float64) float64 { return float64(left) + v/maxX*pw }
	ypos := func(ms float64) float64 { return float64(top) + ph - (math.Log10(ms)-lo)/(hi-lo)*ph }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" font-family="system-ui, sans-serif" font-size="12">`+"\n", width, height)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#ffffff"/>`+"\n", width, height)
	fmt.Fprintf(&b, `<text x="%d" y="24" font-size="15" font-weight="600" fill="#1c1c1a">%s</text>`+"\n", left, esc(title))
	fmt.Fprintf(&b, `<text x="%d" y="42" font-size="11" fill="#6b6b66">%s</text>`+"\n", left, esc(truncate(subtitle, 115)))
	for d := lo; d <= hi; d++ {
		for _, m := range []float64{1, 2, 5} {
			v := m * math.Pow(10, d)
			if math.Log10(v) > hi+1e-9 {
				continue
			}
			y := ypos(v)
			stroke := "#ececea"
			if m == 1 {
				stroke = "#d8d8d4"
			}
			fmt.Fprintf(&b, `<line x1="%d" x2="%.1f" y1="%.1f" y2="%.1f" stroke="%s"/>`+"\n", left, float64(left)+pw, y, y, stroke)
			fmt.Fprintf(&b, `<text x="%d" y="%.1f" text-anchor="end" fill="#6b6b66">%s</text>`+"\n", left-8, y+4, fmtMS(v))
		}
	}
	for i := 0; i <= 5; i++ {
		v := maxX * float64(i) / 5
		x := xpos(v)
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%d" y2="%.1f" stroke="#ececea"/>`+"\n", x, x, top, float64(top)+ph)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#6b6b66">%.0f</text>`+"\n", x, float64(top)+ph+18, v)
	}
	fmt.Fprintf(&b, `<text x="%.1f" y="%d" text-anchor="middle" fill="#1c1c1a">settled trades per second</text>`+"\n", float64(left)+pw/2, height-14)
	fmt.Fprintf(&b, `<text transform="translate(18 %.1f) rotate(-90)" text-anchor="middle" fill="#1c1c1a">order to ledger %s (log scale)</text>`+"\n", float64(top)+ph/2, esc(quantile))

	for i, c := range configs {
		color := compareColors[i%len(compareColors)]
		var pts []string
		for _, r := range c.Report.Runs {
			if v := msf(pick(r)); v > 0 {
				pts = append(pts, fmt.Sprintf("%.1f,%.1f", xpos(r.SettledPerSec), ypos(v)))
			}
		}
		fmt.Fprintf(&b, `<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>`+"\n", color, strings.Join(pts, " "))
		for _, r := range c.Report.Runs {
			v := msf(pick(r))
			if v <= 0 {
				continue
			}
			fill := color
			if !r.Sustained {
				fill = "#ffffff"
			}
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="%s" stroke="%s" stroke-width="1.5"/>`+"\n", xpos(r.SettledPerSec), ypos(v), fill, color)
		}
		ly := float64(top) + 10 + float64(i)*20
		lx := float64(left) + pw + 16
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="2"/>`+"\n", lx, lx+22, ly, ly, color)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" fill="#1c1c1a">%s</text>`+"\n", lx+28, ly+4, esc(c.Name))
	}
	ly := float64(top) + 10 + float64(len(configs))*20 + 6
	fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="#ffffff" stroke="#6b6b66" stroke-width="1.5"/>`+"\n", float64(left)+pw+27, ly)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" fill="#6b6b66">not sustained</text>`+"\n", float64(left)+pw+44, ly+4)
	if maxY >= saturation*0.99 {
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="#6b6b66">10s: histogram ceiling</text>`+"\n", float64(left)+pw+16, ly+24)
	}
	b.WriteString("</svg>\n")
	_, err := io.WriteString(w, b.String())
	return err
}
