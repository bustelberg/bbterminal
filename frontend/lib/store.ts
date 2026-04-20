import { useSyncExternalStore } from 'react';

// Module-scoped state survives component unmount/remount, which is what lets
// a long-running job keep going when the user navigates away from its page.

export type Store<T extends object> = {
  get: () => T;
  set: (updater: Partial<T> | ((prev: T) => Partial<T>)) => void;
  subscribe: (listener: () => void) => () => void;
  use: <U>(selector: (state: T) => U) => U;
};

export function createStore<T extends object>(initial: T): Store<T> {
  let state = initial;
  const listeners = new Set<() => void>();

  const get = () => state;

  const set: Store<T>['set'] = (updater) => {
    const partial =
      typeof updater === 'function' ? (updater as (p: T) => Partial<T>)(state) : updater;
    state = { ...state, ...partial };
    listeners.forEach((l) => l());
  };

  const subscribe = (listener: () => void) => {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  };

  const use = <U>(selector: (state: T) => U): U =>
    useSyncExternalStore(
      subscribe,
      () => selector(state),
      () => selector(state),
    );

  return { get, set, subscribe, use };
}
