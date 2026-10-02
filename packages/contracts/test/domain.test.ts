import { describe, expect, it } from "vitest";
import {
  calculateSessionDuration,
  canTransitionSession,
  describeMissingReview,
  isGoalProgressValid,
  planMultipartUpload,
  validateAnnotationRange,
  MIN_UPLOAD_PART_BYTES,
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
  it("keeps a single part for small files", () => {
    const plan = planMultipartUpload(12_345);
    expect(plan.parts).toEqual([{ partNumber: 1, start: 0, end: 12_345 }]);
  });

  it("splits large files into contiguous non-overlapping ranges", () => {
    const size = MIN_UPLOAD_PART_BYTES * 2 + 123;
    const plan = planMultipartUpload(size, MIN_UPLOAD_PART_BYTES);
    expect(plan.parts).toHaveLength(3);
    expect(plan.parts[0]).toEqual({ partNumber: 1, start: 0, end: MIN_UPLOAD_PART_BYTES });
    expect(plan.parts[1]).toEqual({ partNumber: 2, start: MIN_UPLOAD_PART_BYTES, end: MIN_UPLOAD_PART_BYTES * 2 });
    expect(plan.parts[2]).toEqual({ partNumber: 3, start: MIN_UPLOAD_PART_BYTES * 2, end: size });
  });

  it("enlarges the part size when the part cap would be exceeded", () => {
    const plan = planMultipartUpload(MIN_UPLOAD_PART_BYTES * 12_000, MIN_UPLOAD_PART_BYTES);
    expect(plan.parts.length).toBeLessThanOrEqual(10_000);
    expect(plan.partSize).toBeGreaterThan(MIN_UPLOAD_PART_BYTES);
    expect(plan.parts.at(-1)!.end).toBe(MIN_UPLOAD_PART_BYTES * 12_000);
  });
});
