import React from 'react';
import { motion } from 'motion/react';
import { X } from 'lucide-react';

export function HelpModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-[120] flex items-center justify-center p-6 bg-rm-navy/45 backdrop-blur-sm" role="dialog" aria-modal="true" aria-labelledby="help-title">
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        className="bg-white border border-slate-200 rounded-2xl shadow-2xl max-w-lg w-full max-h-[min(80vh,520px)] overflow-y-auto"
      >
        <div className="flex items-start justify-between gap-3 px-6 py-5 border-b border-slate-100">
          <div>
            <h2 id="help-title" className="text-lg font-serif font-semibold text-slate-900">
              Pulse Monitor — quick reference
            </h2>
            <p className="text-xs text-slate-500 mt-1">Keyboard: use sidebar to switch pages. Station deep links use <code className="text-slate-600">#/stations/&lt;id&gt;</code>.</p>
          </div>
          <button type="button" onClick={onClose} className="p-2 rounded-lg border border-slate-200 text-slate-500 hover:text-slate-800 hover:bg-slate-50" aria-label="Close help">
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="px-6 py-5 text-sm text-slate-700 space-y-4">
          <section>
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-400 mb-2">Stations</h3>
            <p>Use state chips and search to find a stream. <strong className="text-slate-800">View</strong> opens the profile with per-station logs and song spins. Row actions can probe or pause monitoring.</p>
          </section>
          <section>
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-400 mb-2">Live Activity and History</h3>
            <p>Both use the same detection log API. The header search filters client-side by station, title, artist, method, or status. History adds a station scope filter.</p>
          </section>
          <section>
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-400 mb-2">Right rail</h3>
            <p>Snapshot and calendar use live metrics and the current month. Recent lines and province mix are derived from your loaded stations and the latest log slice.</p>
          </section>
          <section>
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-400 mb-2">Fingerprint stack</h3>
            <p>Readiness and paid-lane keys are under <strong className="text-slate-800">Settings</strong>. The monitor degrades gracefully when optional APIs are missing.</p>
          </section>
        </div>
      </motion.div>
    </div>
  );
}
