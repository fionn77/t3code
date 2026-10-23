import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ServerConfigStreamEvent,
  UsageLimitSourceId,
  type UsageLimitSourceSnapshot,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/http";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as UsageLimitSources from "./UsageLimitSources.ts";

const encodeConfigEvent = Schema.encodeSync(ServerConfigStreamEvent);

it.effect("keeps malformed source URLs encodable for config subscribers", () =>
  Effect.gen(function* () {
    const sources = yield* UsageLimitSources.make;
    yield* sources.refresh;
    const snapshots = yield* sources.current;
    expect(snapshots.map((source) => source.label)).toEqual([
      "local-hub",
      "opaque-hub",
      "Named hub",
      "hub.test:8317",
      "Usage limit source",
    ]);
    expect(() =>
      encodeConfigEvent({
        version: 1,
        type: "usageLimitSourcesUpdated",
        payload: { sources: snapshots },
      }),
    ).not.toThrow();
    expect(snapshots.every((source) => source.error === "No management key configured.")).toBe(
      true,
    );
  }).pipe(
    Effect.provide([
      ServerSettings.layerTest({
        usageLimitSources: {
          [UsageLimitSourceId.make("local-hub")]: {
            kind: "cliproxy",
            url: "localhost:8317",
            managementKey: "",
            enabled: true,
          },
          [UsageLimitSourceId.make("opaque-hub")]: {
            kind: "cliproxy",
            url: "file:///tmp/hub",
            managementKey: "",
            enabled: true,
          },
          [UsageLimitSourceId.make("named-hub")]: {
            kind: "cliproxy",
            url: "localhost:8317",
            label: "Named hub",
            managementKey: "",
            enabled: true,
          },
          [UsageLimitSourceId.make("valid-hub")]: {
            kind: "cliproxy",
            url: "https://hub.test:8317",
            managementKey: "",
            enabled: true,
          },
          [UsageLimitSourceId.make(" ")]: {
            kind: "cliproxy",
            url: "localhost:8317",
            managementKey: "",
            enabled: true,
          },
        },
      }),
      Layer.mock(BackgroundPolicy.BackgroundPolicy)({
        shouldRunScopeWork: () => Effect.succeed(false),
      }),
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("Unexpected network request")),
      ),
      NodeCrypto.layer,
    ]),
    Effect.scoped,
  ),
);

const NOW = Date.parse("2026-10-23T20:00:00.000Z");
const CODEX = ProviderDriverKind.make("codex");
const CLAUDE = ProviderDriverKind.make("claudeAgent");

function hub(
  accounts: ReadonlyArray<{
    readonly id: string;
    readonly driver?: ProviderDriverKind;
    readonly windows: ReadonlyArray<{ readonly usedPercent: number; readonly resetsAt?: string }>;
  }>,
): UsageLimitSourceSnapshot {
  return {
    id: UsageLimitSourceId.make("hub"),
    kind: "cliproxy",
    label: "Hub",
    checkedAt: "2026-10-23T19:59:00.000Z",
    accounts: accounts.map((account) => ({
      id: account.id,
      driver: account.driver ?? CODEX,
      usageLimits: {
        checkedAt: "2026-10-23T19:59:00.000Z",
        windows: account.windows.map((window, index) => ({
          id: `window-${index}`,
          kind: index === 0 ? ("session" as const) : ("weekly" as const),
          label: index === 0 ? "Session" : "Weekly",
          ...window,
        })),
      },
    })),
  };
}

it("resumes a hub pool when its first exhausted account resets", () => {
  const sources = [
    hub([
      // Both windows exhausted: usable only once the later one resets.
      {
        id: "a",
        windows: [
          { usedPercent: 100, resetsAt: "2026-10-23T21:00:00.000Z" },
          { usedPercent: 100, resetsAt: "2026-10-25T09:00:00.000Z" },
        ],
      },
      {
        id: "b",
        windows: [
          { usedPercent: 100, resetsAt: "2026-10-23T23:33:00.000Z" },
          { usedPercent: 16, resetsAt: "2026-10-30T19:33:00.000Z" },
        ],
      },
      { id: "claude", driver: CLAUDE, windows: [{ usedPercent: 40 }] },
    ]),
  ];
  expect(UsageLimitSources.hubUsageLimitResetAt(sources, CODEX, NOW)).toBe(
    "2026-10-23T23:33:00.000Z",
  );
});

it("leaves the reset unknown when the hub cannot explain the stop", () => {
  const exhausted = {
    id: "a",
    windows: [{ usedPercent: 100, resetsAt: "2026-10-23T23:33:00.000Z" }],
  };
  for (const accounts of [
    [],
    // Another account still has capacity, so quota does not explain the stop.
    [exhausted, { id: "b", windows: [{ usedPercent: 54, resetsAt: "2026-10-24T01:33:00.000Z" }] }],
    [exhausted, { id: "b", windows: [{ usedPercent: 100 }] }],
    [{ id: "a", windows: [{ usedPercent: 100, resetsAt: "2026-10-23T19:00:00.000Z" }] }],
    [{ ...exhausted, driver: CLAUDE }],
  ]) {
    expect(UsageLimitSources.hubUsageLimitResetAt([hub(accounts)], CODEX, NOW)).toBeNull();
  }
});
