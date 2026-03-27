import IngestButton from './components/IngestButton'

export default async function Home() {
  let items: { id: string; name: string }[] = []
  try {
    const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/items`, {
      cache: 'no-store',
    })
    if (res.ok) {
      const data = await res.json()
      items = data.items ?? []
    }
  } catch {
    // backend unavailable or table not yet created
  }

  return (
    <main className="p-8">
      <h1 className="text-2xl font-bold mb-4">BBTerminal</h1>

      <section>
        <h2 className="text-lg font-semibold mb-2">Long Equity Ingest</h2>
        <p className="text-sm text-gray-500 mb-4">
          Downloads the latest Long Equity Excel files, stores the raw files, and
          loads them into the Supabase schema.
        </p>
        <IngestButton />
      </section>

      {items.length > 0 && (
        <section className="mt-12">
          <h2 className="text-lg font-semibold mb-2">Items</h2>
          <ul className="list-disc pl-6">
            {items.map((item) => (
              <li key={item.id}>{item.name}</li>
            ))}
          </ul>
        </section>
      )}
    </main>
  )
}
