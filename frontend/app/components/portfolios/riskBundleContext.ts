'use client';

import { createContext } from 'react';

export type RiskBundleState = { views: Record<string, unknown>; error: string | null };
export const RiskBundleContext = createContext<RiskBundleState | null>(null);
