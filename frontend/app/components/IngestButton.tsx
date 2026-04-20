'use client'

import { useEffect, useRef } from 'react'

import { ingestStore, startIngest } from '../../lib/stores/ingest'

export default function IngestButton() {
  const running = ingestStore.use((s) => s.running)
  const logs = ingestStore.use((s) => s.log)
  const finished = ingestStore.use((s) => s.finished)
  const logEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = logEndRef.current?.parentElement
    if (el) el.scrollTop = el.scrollHeight
  }, [logs])

  return (
    <div className="mt-8 max-w-2xl">
      <button
        onClick={() => startIngest()}
        disabled={running}
        className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        {running ? 'Running...' : finished ? 'Run Again' : 'Run Long Equity Ingest'}
      </button>

      {logs.length > 0 && (
        <div className="mt-4 p-4 bg-gray-950 text-gray-100 rounded font-mono text-sm max-h-96 overflow-y-auto border border-gray-700">
          {logs.map((entry, i) => (
            <div
              key={i}
              className={
                entry.type === 'error'
                  ? 'text-red-400'
                  : entry.type === 'done'
                  ? 'text-green-400 font-semibold'
                  : 'text-gray-200'
              }
            >
              {entry.message || '\u00a0'}
            </div>
          ))}
          <div ref={logEndRef} />
        </div>
      )}
    </div>
  )
}
