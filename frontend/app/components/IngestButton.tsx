'use client'

import { useEffect, useRef, useState } from 'react'

type LogEntry = {
  type: 'info' | 'done' | 'error'
  message: string
}

export default function IngestButton() {
  const [running, setRunning] = useState(false)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [finished, setFinished] = useState(false)
  const logEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  const handleClick = async () => {
    setRunning(true)
    setFinished(false)
    setLogs([])

    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL}/api/ingest/long-equity`,
        { method: 'POST' }
      )

      if (!response.ok || !response.body) {
        throw new Error(`Request failed with status ${response.status}`)
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          try {
            const entry = JSON.parse(line.slice(6)) as LogEntry
            setLogs((prev) => [...prev, entry])
            if (entry.type === 'done') setFinished(true)
          } catch {
            // ignore malformed SSE lines
          }
        }
      }
    } catch (err) {
      setLogs((prev) => [
        ...prev,
        { type: 'error', message: String(err) },
      ])
    } finally {
      setRunning(false)
    }
  }

  return (
    <div className="mt-8 max-w-2xl">
      <button
        onClick={handleClick}
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
