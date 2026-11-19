package loadgen

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// series is one line on the chart.
type series struct {
	name  string
	color string
	dash  string
	value func(Result) time.Duration
}

var plotSeries = []series{
	{"order to ledger p50", "#2f6fdb", "", func(r Result) time.Duration { return r.Pipeline.OrderToLedger.P50 }},
	{"order to ledger p99", "#d1495b", "", func(r Result) time.Duration { return r.Pipeline.OrderToLedger.P99 }},
	{"gateway ack p99 (client)", "#6b6b66", "5 4", func(r Result) time.Duration { return r.Ack.P99 }},
}

// PlotSVG writes latency (y, ms, log scale) against achieved throughput
// (x, trades/s) for a sweep: the knee is where p99 turns upward. Runs that
// were not sustained are marked with a hollow point.
func (r Report) PlotSVG(w io.Writer, title, subtitle string) error {
	const (
		width, height          = 760, 440
		left, right, top, bott = 70, 190, 62, 56
	)
	pw, ph := float64(width-left-right), float64(height-top-bott)
	runs := r.Runs
	if len(runs) == 0 {
		return fmt.Errorf("no runs to plot")
	}

	maxX := 0.0
	minY, maxY := math.Inf(1), 0.0
	for _, x := range runs {
		maxX = math.Max(maxX, x.SettledPerSec)
		for _, s := range plotSeries {
			v := msf(s.value(x))
			if v > 0 {
				minY, maxY = math.Min(minY, v), math.Max(maxY, v)
			}
		}
	}
	if maxX == 0 || maxY == 0 {
		return fmt.Errorf("nothing measured to plot")
	}
	maxX = niceCeil(maxX * 1.05)
	lo, hi := math.Floor(math.Log10(minY)), math.Ceil(math.Log10(maxY))
	if hi == lo {
		hi = lo + 1
	}
	xpos := func(v float64) float64 { return float64(left) + v/maxX*pw }
	ypos := func(ms float64) float64 {
		return float64(top) + ph - (math.Log10(ms)-lo)/(hi-lo)*ph
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" font-family="system-ui, sans-serif" font-size="12">`+"\n", width, height)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#ffffff"/>`+"\n", width, height)
	fmt.Fprintf(&b, `<text x="%d" y="24" font-size="15" font-weight="600" fill="#1c1c1a">%s</text>`+"\n", left, esc(title))
	fmt.Fprintf(&b, `<text x="%d" y="42" font-size="11" fill="#6b6b66">%s</text>`+"\n", left, esc(truncate(subtitle, 110)))

	// Y grid at each decade and its 2x and 5x.
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
	// X ticks.
	for i := 0; i <= 5; i++ {
		v := maxX * float64(i) / 5
		x := xpos(v)
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%d" y2="%.1f" stroke="#ececea"/>`+"\n", x, x, top, float64(top)+ph)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#6b6b66">%.0f</text>`+"\n", x, float64(top)+ph+18, v)
	}
	fmt.Fprintf(&b, `<text x="%.1f" y="%d" text-anchor="middle" fill="#1c1c1a">settled trades per second</text>`+"\n", float64(left)+pw/2, height-14)
	fmt.Fprintf(&b, `<text transform="translate(18 %.1f) rotate(-90)" text-anchor="middle" fill="#1c1c1a">latency (log scale)</text>`+"\n", float64(top)+ph/2)

	for i, s := range plotSeries {
		var pts []string
		for _, x := range runs {
			if v := msf(s.value(x)); v > 0 {
				pts = append(pts, fmt.Sprintf("%.1f,%.1f", xpos(x.SettledPerSec), ypos(v)))
			}
		}
		dash := ""
		if s.dash != "" {
			dash = fmt.Sprintf(` stroke-dasharray="%s"`, s.dash)
		}
		fmt.Fprintf(&b, `<polyline fill="none" stroke="%s" stroke-width="2"%s points="%s"/>`+"\n", s.color, dash, strings.Join(pts, " "))
		for _, x := range runs {
			v := msf(s.value(x))
			if v <= 0 {
				continue
			}
			fill := s.color
			if !x.Sustained {
				fill = "#ffffff"
			}
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="%s" stroke="%s" stroke-width="1.5"/>`+"\n",
				xpos(x.SettledPerSec), ypos(v), fill, s.color)
		}
		ly := float64(top) + 10 + float64(i)*20
		lx := float64(left) + pw + 16
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="2"%s/>`+"\n", lx, lx+22, ly, ly, s.color, dash)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" fill="#1c1c1a">%s</text>`+"\n", lx+28, ly+4, esc(s.name))
	}
	if maxY >= saturation*0.99 {
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="#6b6b66">at or above 10s: histogram ceiling</text>`+"\n",
			float64(left)+pw+16, float64(top)+10+float64(len(plotSeries))*20+30)
	}
	ly := float64(top) + 10 + float64(len(plotSeries))*20 + 6
	fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="#ffffff" stroke="#6b6b66" stroke-width="1.5"/>`+"\n", float64(left)+pw+27, ly)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" fill="#6b6b66">not sustained</text>`+"\n", float64(left)+pw+44, ly+4)
	b.WriteString("</svg>\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// saturation is the top histogram bucket in ms: quantiles there mean "at least".
const saturation = 10_000.0

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func msf(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func fmtMS(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%gs", v/1000)
	}
	return fmt.Sprintf("%gms", v)
}

// niceCeil rounds up to 1, 2 or 5 times a power of ten.
func niceCeil(v float64) float64 {
	p := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 5, 10} {
		if m*p >= v {
			return m * p
		}
	}
	return 10 * p
}

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
