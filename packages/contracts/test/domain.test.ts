import { describe, expect, it } from "vitest";
import {
  calculateSessionDuration,
  canTransitionSession,
  describeMissingReview,
  isGoalProgressValid,
  partByteRange,
  planMultipart,
  validateAnnotationRange,
} from "../src/index.js";

describe("session state machine", () => {
  it("allows the required completion transition", () => {
    expect(canTransitionSession("IN_REVIEW", "COMPLETED")).toBe(true);
    expect(canTransitionSession("DRAFT", "COMPLETED")).toBe(false);
  });
});

describe("annotation range", () => {
  it("rejects ranges under 100ms and outside media", () => {
    expect(validateAnnotationRange(100, 150, 1000)).toMatchObject({ ok: false });
    expect(validateAnnotationRange(900, 1100, 1000)).toMatchObject({ ok: false });
    expect(validateAnnotationRange(100, 250, 1000)).toEqual({ ok: true });
  });
});

describe("review completion", () => {
  it("returns every missing item instead of a generic failure", () => {
    expect(
      describeMissingReview({
        readyMediaCount: 0,
        annotationCount: 0,
        noIssues: false,
        nextFocus: "",
        openGoalCount: 0,
        newGoalCount: 0,
        progressUpdateCount: 0,
      }),
    ).toHaveLength(4);
  });
});

describe("goal values", () => {
  it("suggests achieved only when actual reaches target", () => {
    expect(isGoalProgressValid(90, 88)).toBe(true);
    expect(isGoalProgressValid(87, 88)).toBe(false);
  });

  it("sums only valid media durations", () => {
    expect(calculateSessionDuration([1000, null, 2500, -1])).toBe(3500);
  });
});

describe("multipart upload planning", () => {
  it("uses the preferred part size for small files and keeps the last part shorter", () => {
    const plan = planMultipart(20 * 1024 * 1024);
    expect(plan.partCount).toBe(3);
    expect(plan.partSizeBytes).toBe(8 * 1024 * 1024);
    expect(partByteRange(3, plan.partSizeBytes, 20 * 1024 * 1024)).toEqual({
      start: 16 * 1024 * 1024,
      end: 20 * 1024 * 1024,
    });
  });

  it("never plans parts below 5 MiB except the last", () => {
    const size = 9 * 1024 * 1024;
    const plan = planMultipart(size);
    expect(plan.partCount).toBe(2);
    expect(plan.partSizeBytes).toBe(8 * 1024 * 1024);
    expect(partByteRange(1, plan.partSizeBytes, size)).toEqual({ start: 0, end: 8 * 1024 * 1024 });
    expect(partByteRange(2, plan.partSizeBytes, size)).toEqual({ start: 8 * 1024 * 1024, end: size });
  });

  it("grows part size for huge files to stay within the part limit", () => {
    const size = 500 * 1024 * 1024;
    const plan = planMultipart(size);
    expect(plan.partCount).toBeLessThanOrEqual(10_000);
    expect(plan.partSizeBytes).toBeGreaterThanOrEqual(5 * 1024 * 1024);
  });

  it("rejects non-positive sizes", () => {
    expect(() => planMultipart(0)).toThrow();
  });
});
