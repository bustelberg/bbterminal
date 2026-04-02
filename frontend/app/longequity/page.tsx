import LongEquityInsight from '../components/LongEquityInsight';

export type Snapshot = {
  target_date: string;
};

export default async function LongEquityPage() {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';
  let snapshots: Snapshot[] = [];
  try {
    const res = await fetch(`${apiUrl}/api/longequity/snapshots`, {
      cache: 'no-store',
    });
    if (res.ok) snapshots = await res.json();
  } catch {
    // backend unavailable
  }

  return <LongEquityInsight snapshots={snapshots} />;
}
