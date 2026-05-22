/**
 * Backend API base URL. Reads `NEXT_PUBLIC_API_URL` (set in
 * `frontend/.env.local` for dev and Vercel env for prod), falling back
 * to localhost:8000.
 *
 * Single source of truth — previously each component (33+ files)
 * declared its own copy of this same line. Import from here so the
 * fallback can ever be changed in exactly one place.
 */
export const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';
