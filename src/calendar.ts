/** VERIFIED: Croner owns timezone transitions and field evaluation. The calendar fixtures pin the public dialect and Berlin transitions. */
import { Cron } from "croner";
import { invalid } from "./validation.js";

export function calendar(expression: string, timezone = "UTC"): Cron {
  if (
    typeof expression !== "string" ||
    expression.length > 256 ||
    typeof timezone !== "string" ||
    !timezone ||
    timezone.length > 96
  )
    invalid("Expected a cron expression and an IANA timezone");
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5 && fields.length !== 6)
    invalid("Cron requires 5 or 6 fields");
  const bounds = [
    [0, 59],
    [0, 59],
    [0, 23],
    [1, 31],
    [1, 12],
    [0, 7],
  ].slice(6 - fields.length);
  fields.forEach((field, i) => {
    for (const term of field.split(",")) {
      const m = /^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/.exec(term);
      if (
        !m ||
        (m[2] && (+m[2] < 1 || +m[2] > bounds[i][1] - bounds[i][0] + 1))
      )
        invalid("Invalid cron term: " + term);
      if (m[1] !== "*") {
        const [a, b = a] = m[1].split("-").map(Number);
        if (a < bounds[i][0] || b > bounds[i][1] || a > b)
          invalid("Cron field outside range");
      }
    }
  });
  try {
    new Intl.DateTimeFormat("en", { timeZone: timezone });
    if (/^[+-]/.test(timezone)) invalid("Expected an IANA timezone");
    return new Cron(fields.join(" "), {
      timezone,
      domAndDow: false,
      mode: "5-or-6-parts",
    });
  } catch {
    return invalid("Invalid calendar or timezone");
  }
}
export function nextOccurrence(
  cron: string,
  timezone: string,
  after: number | Date,
): number | null {
  const date = new Date(after);
  if (!Number.isFinite(date.getTime())) invalid("Invalid reference date");
  return calendar(cron, timezone).nextRun(date)?.getTime() ?? null;
}
