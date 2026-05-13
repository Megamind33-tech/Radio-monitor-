import React from 'react';
import { Radio, Headphones, AlertTriangle } from 'lucide-react';

export interface NotificationItem {
  id: string;
  title: string;
  detail?: string;
  tone?: 'info' | 'warn';
  actionLabel?: string;
  onAction?: () => void;
}

export function NotificationsPanel({
  open,
  onClose,
  items,
}: {
  open: boolean;
  onClose: () => void;
  items: NotificationItem[];
}) {
  if (!open) return null;
  return (
    <>
      <button type="button" className="fixed inset-0 z-[115] cursor-default bg-transparent" aria-label="Dismiss" onClick={onClose} />
      <div className="fixed top-20 right-6 z-[116] w-[min(100vw-2rem,340px)] rm-card p-0 shadow-xl border border-slate-200 overflow-hidden">
        <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between">
          <span className="text-sm font-semibold text-slate-900">Notifications</span>
          <button type="button" className="text-xs text-slate-500 hover:text-rm-indigo" onClick={onClose}>
            Close
          </button>
        </div>
        <ul className="max-h-[min(60vh,360px)] overflow-y-auto divide-y divide-slate-100">
          {items.length === 0 ? (
            <li className="px-4 py-8 text-center text-sm text-slate-500">Nothing that needs attention right now.</li>
          ) : (
            items.map((it) => (
              <li key={it.id} className="px-4 py-3 text-sm">
                <div className="flex items-start gap-2">
                  {it.tone === 'warn' ? (
                    <AlertTriangle className="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
                  ) : it.id.startsWith('audio') ? (
                    <Headphones className="w-4 h-4 text-rm-indigo shrink-0 mt-0.5" />
                  ) : (
                    <Radio className="w-4 h-4 text-slate-400 shrink-0 mt-0.5" />
                  )}
                  <div className="min-w-0 flex-1">
                    <div className="font-medium text-slate-800">{it.title}</div>
                    {it.detail ? <div className="text-xs text-slate-500 mt-0.5">{it.detail}</div> : null}
                    {it.actionLabel && it.onAction ? (
                      <button
                        type="button"
                        className="mt-2 text-xs font-semibold text-rm-indigo hover:underline"
                        onClick={() => {
                          it.onAction?.();
                          onClose();
                        }}
                      >
                        {it.actionLabel}
                      </button>
                    ) : null}
                  </div>
                </div>
              </li>
            ))
          )}
        </ul>
      </div>
    </>
  );
}
