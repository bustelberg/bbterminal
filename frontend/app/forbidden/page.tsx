'use client';

import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import { Suspense } from 'react';
import LoadingDots from '../components/LoadingDots';

function ForbiddenContent() {
  const params = useSearchParams();
  const from = params.get('from');
  return (
    <div className="px-8 py-12 max-w-2xl">
      <div className="bg-[#151821] border border-rose-500/20 rounded-xl p-6">
        <div className="flex items-start gap-3">
          <div className="shrink-0 w-9 h-9 rounded-full bg-rose-500/15 border border-rose-500/30 flex items-center justify-center">
            <svg viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4 text-rose-400">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm0-13a1 1 0 011 1v4a1 1 0 11-2 0V6a1 1 0 011-1zm0 9a1 1 0 100 2 1 1 0 000-2z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="flex-1">
            <h1 className="text-lg font-semibold text-white">No access to this page</h1>
            <p className="text-sm text-gray-400 mt-1.5 leading-relaxed">
              {from ? (
                <>
                  Your account doesn&apos;t have permission to view{' '}
                  <span className="font-mono text-gray-300">{from}</span>. If you believe this is wrong, ask the workspace admin to grant you access.
                </>
              ) : (
                <>Your account doesn&apos;t have permission to view this page. If you believe this is wrong, ask the workspace admin to grant you access.</>
              )}
            </p>
            <div className="mt-4 flex gap-2">
              <Link
                href="/"
                className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
              >
                Go to home
              </Link>
              <Link
                href="/earnings"
                className="px-4 py-2 rounded-lg text-sm font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors"
              >
                Go to Earnings
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function Forbidden() {
  // useSearchParams() needs a Suspense boundary in app router.
  return (
    <Suspense fallback={<div className="px-8 py-12 text-sm text-gray-500"><LoadingDots label="Loading" /></div>}>
      <ForbiddenContent />
    </Suspense>
  );
}
