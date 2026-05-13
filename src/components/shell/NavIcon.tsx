import React from 'react';
import { motion } from 'motion/react';

export function NavIcon({
  icon,
  active,
  onClick,
  label,
}: {
  icon: React.ReactNode;
  active: boolean;
  onClick: () => void;
  label: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`relative w-full flex items-center gap-3 px-3 py-2.5 rounded-xl transition-all duration-200 text-sm font-medium ${
        active
          ? 'bg-indigo-500/20 text-white border border-white/12 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.06)]'
          : 'text-white/55 hover:text-white hover:bg-white/[0.07] border border-transparent'
      }`}
    >
      {active && (
        <motion.div
          layoutId="nav-active"
          className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5 rounded-full bg-indigo-300 shadow-[0_0_10px_rgba(165,180,252,0.85)]"
        />
      )}
      <span className={`${active ? 'text-indigo-200' : 'text-white/45'} transition-colors`}>{icon}</span>
      <span>{label}</span>
    </button>
  );
}
