# Seine Travel

This repository hosts the static site for Seine.travel.

## GetYourGuide Widget

The site integrates GetYourGuide's new widget using `data-gyg-widget="activities"`. The widget markup looks like:

```html
<div
  data-gyg-widget="activities"
  data-gyg-q="seine"
  data-gyg-partner-id="X3LLOUG"
  data-gyg-locale-code="en-US"
  data-gyg-number-of-items="8">
</div>
```

Styling for this container lives in `assets/css/style.css` and ensures the widget height adjusts on mobile and desktop so the attribution text stays visible.
