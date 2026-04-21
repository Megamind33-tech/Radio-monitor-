import React, { useState, useEffect } from 'react';
import { 
  Radio, 
  Activity, 
  Settings, 
  History, 
  Plus, 
  RefreshCw, 
  ExternalLink,
  Search,
  Copy,
  CheckCircle2,
  AlertCircle,
  BarChart3,
  Globe,
  Music
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { 
  AreaChart, 
  Area, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer 
} from 'recharts';

// --- Types ---
interface Station {
  id: string;
  name: string;
  country: string;
  streamUrl: string;
  isActive: boolean;
  currentNowPlaying?: {
    title: string;
    artist: string;
    updatedAt: string;
  };
}

interface DetectionLog {
  id: string;
  stationId: string;
  observedAt: string;
  detectionMethod: string;
  artistFinal: string;
  titleFinal: string;
  status: string;
  acoustidScore?: number;
}

interface Metrics {
  total_detections: number;
  match_rate: number;
  errors_count: number;
}

// --- Components ---

export default function App() {
  const [stations, setStations] = useState<Station[]>([]);
  const [logs, setLogs] = useState<DetectionLog[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<'stations' | 'history' | 'settings'>('stations');
  const [isAddingStation, setIsAddingStation] = useState(false);

  const fetchData = async () => {
    try {
      const [stRes, metRes] = await Promise.all([
        fetch('/api/stations'),
        fetch('/api/metrics/summary')
      ]);
      const stData = await stRes.json();
      const metData = await metRes.json();
      setStations(stData);
      setMetrics(metData);
    } catch (error) {
      console.error("Failed to fetch data", error);
    } finally {
      setLoading(false);
    }
  };

  const fetchLogs = async (id: string) => {
    try {
      const res = await fetch(`/api/stations/${id}/logs`);
      const data = await res.json();
      setLogs(data);
    } catch (error) {
      console.error("Failed to fetch logs", error);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-brand-bg text-gray-100 selection:bg-brand-cyan/30">
      {/* Sidebar / Nav */}
      <nav className="fixed left-0 top-0 h-full w-20 border-r border-white/5 bg-black/40 backdrop-blur-xl flex flex-col items-center py-8 gap-10 z-50">
        <div className="p-3 bg-brand-cyan/20 rounded-2xl border border-brand-cyan/30 shadow-[0_0_20px_rgba(0,242,255,0.2)]">
          <Radio className="w-8 h-8 text-brand-cyan" />
        </div>
        
        <div className="flex flex-col gap-6 flex-1">
          <NavIcon icon={<Activity className="w-6 h-6" />} active={activeTab === 'stations'} onClick={() => setActiveTab('stations')} label="Monitor" />
          <NavIcon icon={<History className="w-6 h-6" />} active={activeTab === 'history'} onClick={() => setActiveTab('history')} label="History" />
          <NavIcon icon={<Settings className="w-6 h-6" />} active={activeTab === 'settings'} onClick={() => setActiveTab('settings')} label="Settings" />
        </div>

        <button className="p-3 text-gray-500 hover:text-white transition-colors">
          <Plus className="w-6 h-6" onClick={() => setIsAddingStation(true)} />
        </button>
      </nav>

      {/* Main Content */}
      <main className="pl-32 pr-8 py-12 max-w-7xl mx-auto">
        {/* Header */}
        <header className="mb-12 flex justify-between items-end">
          <div>
            <h1 className="text-4xl font-bold tracking-tight mb-2 flex items-center gap-3">
              Radio Pulse <span className="text-brand-cyan">Monitor</span>
            </h1>
            <p className="text-gray-400">Production-grade MVP for radio now-playing detection.</p>
          </div>
          
          <div className="flex gap-4">
            <MetricCard 
              label="Match Rate" 
              value={metrics ? `${(metrics.match_rate * 100).toFixed(1)}%` : '--'} 
              sub="Avg confidence"
            />
            <MetricCard 
              label="Monitoring" 
              value={stations.length.toString()} 
              sub="Active stations"
            />
          </div>
        </header>

        {activeTab === 'stations' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <AnimatePresence mode="popLayout">
              {stations.map((station) => (
                <StationCard 
                  key={station.id} 
                  station={station} 
                  onProbe={() => fetchData()} 
                />
              ))}
            </AnimatePresence>
          </div>
        )}

        {activeTab === 'history' && (
          <div className="bg-white/5 border border-white/10 rounded-3xl p-8 backdrop-blur-sm">
             <div className="flex justify-between items-center mb-6">
                <h2 className="text-xl font-semibold flex items-center gap-2">
                  <History className="w-5 h-5 text-brand-purple" />
                  Recent Detection Logs
                </h2>
                <div className="flex gap-2">
                   <select className="bg-black/40 border border-white/10 rounded-lg px-3 py-1 text-sm outline-none">
                      <option>All Stations</option>
                      {stations.map(s => <option key={s.id}>{s.name}</option>)}
                   </select>
                </div>
             </div>
             
             <div className="overflow-x-auto">
                <table className="w-full text-left">
                  <thead>
                    <tr className="border-b border-white/5 text-gray-500 text-sm">
                      <th className="pb-4 font-medium">Time</th>
                      <th className="pb-4 font-medium">Station</th>
                      <th className="pb-4 font-medium">Track Info</th>
                      <th className="pb-4 font-medium">Method</th>
                      <th className="pb-4 font-medium">Status</th>
                    </tr>
                  </thead>
                  <tbody className="text-sm">
                    {/* Placeholder for real logs */}
                    <tr className="border-b border-white/5 hover:bg-white/5 transition-colors group">
                      <td className="py-4 text-gray-400">12:45:02</td>
                      <td className="py-4 font-medium">BBC Radio 1</td>
                      <td className="py-4">
                        <div className="flex flex-col">
                          <span className="font-semibold group-hover:text-brand-cyan transition-colors">Flowers</span>
                          <span className="text-xs text-gray-500">Miley Cyrus</span>
                        </div>
                      </td>
                      <td className="py-4">
                        <span className="px-2 py-0.5 bg-brand-cyan/10 text-brand-cyan rounded-full text-[10px] uppercase font-bold">AcoustID</span>
                      </td>
                      <td className="py-4">
                        <CheckCircle2 className="w-4 h-4 text-green-500" />
                      </td>
                    </tr>
                    {/* Map real logs here if available */}
                  </tbody>
                </table>
             </div>
          </div>
        )}

        {activeTab === 'settings' && (
          <div className="max-w-2xl bg-white/5 border border-white/10 rounded-3xl p-10">
            <h2 className="text-xl font-semibold mb-8 flex items-center gap-2">
              <Settings className="w-5 h-5 text-gray-400" />
              Environment Configuration
            </h2>
            
            <div className="space-y-6">
               <div className="space-y-2">
                  <label className="text-sm font-medium text-gray-400">AcoustID API Key</label>
                  <input type="password" value="••••••••••••••••" readOnly className="w-full bg-black/40 border border-white/10 rounded-xl px-4 py-3 text-brand-cyan outline-none" />
               </div>
               <div className="space-y-2">
                  <label className="text-sm font-medium text-gray-400">MusicBrainz User Agent</label>
                  <input type="text" value="RadioPulseMonitor/1.0.0" readOnly className="w-full bg-black/40 border border-white/10 rounded-xl px-4 py-3 text-gray-300 outline-none" />
               </div>
               <div className="pt-4 border-t border-white/5 flex gap-4">
                  <button className="flex-1 bg-brand-purple hover:bg-brand-purple/80 text-white font-semibold py-3 rounded-xl transition-all">
                    Save Changes
                  </button>
                  <button className="flex-1 bg-white/5 hover:bg-white/10 border border-white/10 text-white font-semibold py-3 rounded-xl transition-all">
                    Test Connections
                  </button>
               </div>
            </div>
          </div>
        )}
      </main>

      {/* Modals */}
      <AnimatePresence>
        {isAddingStation && (
          <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 bg-black/80 backdrop-blur-md">
            <motion.div 
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 20 }}
              className="bg-zinc-900 border border-white/10 w-full max-w-lg rounded-[2rem] overflow-hidden shadow-2xl"
            >
              <div className="px-8 py-10">
                <h3 className="text-2xl font-bold mb-2">Register Station</h3>
                <p className="text-gray-400 mb-8 text-sm">Add a new radio stream to monitor.</p>
                
                <form className="space-y-6" onSubmit={(e) => { e.preventDefault(); setIsAddingStation(false); fetchData(); }}>
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-gray-500">Station Name</label>
                    <input required name="name" type="text" placeholder="e.g. Worldwide FM" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none focus:border-brand-cyan transition-colors" />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-gray-500">Stream URL (Direct MP3/AAC/M3U)</label>
                    <input required name="streamUrl" type="url" placeholder="https://icecast.example.com/stream" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none focus:border-brand-cyan transition-colors" />
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-500">Country</label>
                      <input required name="country" type="text" placeholder="USA" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none" />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-500">Poll (secs)</label>
                      <input required name="pollIntervalSeconds" type="number" defaultValue="60" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none" />
                    </div>
                  </div>
                  
                  <div className="flex gap-4 pt-4">
                    <button type="button" onClick={() => setIsAddingStation(false)} className="flex-1 py-4 text-gray-400 font-medium hover:text-white">Cancel</button>
                    <button type="submit" className="flex-1 bg-brand-cyan text-black font-bold py-4 rounded-2xl hover:brightness-110 shadow-[0_0_20px_rgba(0,242,255,0.3)]">Add Station</button>
                  </div>
                </form>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}

function NavIcon({ icon, active, onClick, label }: any) {
  return (
    <button 
      onClick={onClick}
      className={`group relative p-3 rounded-xl transition-all duration-300 ${active ? 'bg-brand-purple/20 text-brand-purple' : 'text-gray-500 hover:text-gray-300 hover:bg-white/5'}`}
    >
      {icon}
      <span className="absolute left-20 bg-black border border-white/10 text-[10px] text-white px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none">
        {label}
      </span>
      {active && <motion.div layoutId="nav-active" className="absolute -left-5 top-1/4 w-1 h-1/2 bg-brand-purple rounded-full shadow-[0_0_10px_rgba(112,0,255,0.8)]" />}
    </button>
  );
}

function MetricCard({ label, value, sub }: any) {
  return (
    <div className="bg-white/5 border border-white/10 rounded-2xl p-4 flex flex-col items-center min-w-[120px] backdrop-blur-sm">
      <span className="text-[10px] uppercase font-bold text-gray-500 mb-1">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      <span className="text-[10px] text-gray-600 mt-1">{sub}</span>
    </div>
  );
}

function StationCard({ station, onProbe }: { station: Station, onProbe: () => void }) {
  const [probing, setProbing] = useState(false);
  const [copied, setCopied] = useState(false);
  const np = station.currentNowPlaying;

  const handleProbe = async () => {
    setProbing(true);
    await fetch(`/api/stations/${station.id}/probe`, { method: 'POST' });
    onProbe();
    setTimeout(() => setProbing(false), 2000);
  };

  const copyToClipboard = () => {
    navigator.clipboard.writeText(station.streamUrl);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <motion.div 
      layout
      className="group bg-white/5 border border-white/10 hover:border-brand-cyan/30 rounded-[2.5rem] p-8 transition-all hover:bg-white/10 flex flex-col gap-8 relative overflow-hidden"
    >
      <div className="absolute top-0 right-0 p-8">
        <div className={`w-2 h-2 rounded-full ${station.isActive ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
      </div>

      <div className="flex justify-between items-start">
        <div className="flex gap-4 items-center">
          <div className="w-12 h-12 bg-black/40 rounded-2xl flex items-center justify-center border border-white/5">
            <Globe className="w-6 h-6 text-gray-400" />
          </div>
          <div className="flex flex-col">
            <h3 className="text-xl font-bold">{station.name}</h3>
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <span className="px-1.5 py-0.5 bg-white/5 rounded border border-white/10 uppercase">{station.country}</span>
              <span>•</span>
              <span>{station.isActive ? 'Live Monitoring' : 'Inactive'}</span>
            </div>
          </div>
        </div>
        
        <button 
          onClick={handleProbe}
          disabled={probing}
          className="p-3 bg-black/40 border border-white/10 rounded-2xl hover:bg-brand-cyan hover:text-black transition-all group/btn"
        >
          <RefreshCw className={`w-5 h-5 ${probing ? 'animate-spin' : 'group-hover/btn:rotate-180 transition-transform duration-500'}`} />
        </button>
      </div>

      <div className="space-y-4">
        <div className="bg-black/30 rounded-3xl p-6 border border-white/5 space-y-4">
          <div className="flex items-center justify-between text-[10px] text-gray-500 font-bold uppercase tracking-widest">
            <span>Current Stream</span>
            <div className="flex items-center gap-1 text-brand-cyan">
               <BarChart3 className="w-3 h-3" />
               LIVE
            </div>
          </div>

          {np ? (
            <div className="flex items-center gap-4 animate-in fade-in slide-in-from-bottom-2 duration-500">
              <div className="w-16 h-16 bg-gradient-to-br from-brand-cyan/20 to-brand-purple/20 rounded-2xl border border-white/10 flex items-center justify-center">
                <Music className="w-8 h-8 text-brand-cyan" />
              </div>
              <div className="overflow-hidden">
                <h4 className="text-xl font-bold truncate leading-tight mb-1">{np.title}</h4>
                <p className="text-gray-400 truncate">{np.artist}</p>
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-4 opacity-50">
               <div className="w-16 h-16 bg-white/5 rounded-2xl animate-pulse" />
               <div className="space-y-2">
                  <div className="w-32 h-4 bg-white/5 rounded animate-pulse" />
                  <div className="w-24 h-3 bg-white/5 rounded animate-pulse" />
               </div>
            </div>
          )}
        </div>

        <div className="bg-black/20 rounded-2xl p-4 border border-white/5 flex flex-col gap-2">
          <div className="flex justify-between items-center text-[10px] text-gray-500 font-bold uppercase tracking-widest">
            <span>Stream Source</span>
            <span className="font-mono lowercase opacity-50">{station.id}</span>
          </div>
          <div className="flex items-center gap-2 group/url">
            <p className="text-xs text-gray-400 truncate font-mono flex-1">{station.streamUrl}</p>
            <button 
              onClick={copyToClipboard}
              className={`p-1.5 rounded-lg transition-all ${copied ? 'bg-green-500/20 text-green-500' : 'bg-white/5 text-gray-500 hover:text-white'}`}
            >
              {copied ? <CheckCircle2 className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
            </button>
          </div>
        </div>
      </div>

      <div className="flex justify-end items-center px-2">
        <a href={station.streamUrl} target="_blank" rel="noopener noreferrer" className="text-xs text-gray-400 hover:text-brand-cyan flex items-center gap-1.5 transition-colors font-medium">
          Open Direct Stream <ExternalLink className="w-3.5 h-3.5" />
        </a>
      </div>
    </motion.div>
  );
}
