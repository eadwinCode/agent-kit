import { vi } from "vitest";
export type InngestController = {
  push: (chunk: unknown) => void;
  setState: (s: unknown) => void;
  setError: (e: unknown) => void;
  getOptions: () => unknown;
  reset: () => void;
};

let data: unknown[] = [];
let state: unknown = "Inactive";
let error: unknown = undefined;
let options: unknown = undefined;

export const controller: InngestController = {
  push: (chunk: unknown) => (data = [...data, chunk]),
  setState: (s: unknown) => (state = s),
  setError: (e: unknown) => (error = e),
  getOptions: () => options,
  reset: () => {
    data = [];
    state = "Inactive";
    error = undefined;
    options = undefined;
  },
};

// Vitest module mock shim
vi.mock("@inngest/realtime/hooks", () => {
  return {
    useInngestSubscription: (opts: unknown) => {
      options = opts;
      return { data, state, error };
    },
  };
});

export {};
