#!/usr/bin/env python3
"""
OnlineRadioBox Playlist Backfill Scraper
Fetches real playlist history from onlineradiobox.com for all Zambian stations
Backfills missed playlists and covers gaps in the database
"""

import requests
import json
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import sys
import re

class ORBPlaylistScraper:
    def __init__(self, db_path="/opt/radio-monitor/prisma/dev_runtime.db"):
        self.db_path = db_path
        self.base_url = "https://onlineradiobox.com"
        self.track_server = "https://scraper2.onlineradiobox.com/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://onlineradiobox.com/'
        })

    def get_station_url(self, station_id, station_name):
        """Convert station ID to ORB URL"""
        # Example: zm_orb_zm.chikuni -> https://onlineradiobox.com/zm/chikuni/
        if 'orb' in station_id:
            # Extract the part after 'zm.' or 'zm_'
            parts = station_id.split('.')
            if len(parts) >= 2:
                station_slug = parts[-1]
                return f"https://onlineradiobox.com/zm/{station_slug}/"
        return None

    def scrape_track_history(self, station_url, station_id, max_pages=5):
        """Scrape track history from ORB station page"""
        tracks = []

        try:
            print(f"[*] Scraping {station_url}...")
            response = self.session.get(station_url, timeout=15)
            response.encoding = 'utf-8'

            if response.status_code != 200:
                print(f"[-] Failed to fetch {station_url} (status: {response.status_code})")
                return tracks

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find track history table
            history_table = soup.find('table', class_=['track_history', 'station_tracks'])

            if not history_table:
                # Try to find by ID
                history_table = soup.find('table', id=['track_history', 'station_tracks'])

            if history_table:
                # Extract rows from table
                rows = history_table.find_all('tr')

                for row in rows[:50]:  # Get up to 50 tracks
                    try:
                        cells = row.find_all('td')
                        if len(cells) >= 1:
                            # Get track link
                            track_link = row.find('a')
                            if track_link:
                                track_text = track_link.get_text(strip=True)

                                # Parse artist - track format
                                if ' - ' in track_text:
                                    artist, title = track_text.split(' - ', 1)
                                else:
                                    artist = 'Unknown'
                                    title = track_text

                                # Get timestamp if available
                                time_elem = row.find('span', class_='time')
                                timestamp = time_elem.get_text(strip=True) if time_elem else datetime.now().isoformat()

                                tracks.append({
                                    'artist': artist.strip(),
                                    'title': title.strip(),
                                    'timestamp': timestamp,
                                    'source': 'orb_history'
                                })
                    except Exception as e:
                        print(f"  [!] Error parsing row: {e}")
                        continue

            else:
                # Try alternate selectors
                track_items = soup.find_all('div', class_=['track', 'song-item', 'playlist-item'])

                for item in track_items[:50]:
                    try:
                        text = item.get_text(strip=True)
                        if ' - ' in text:
                            artist, title = text.split(' - ', 1)
                            tracks.append({
                                'artist': artist.strip(),
                                'title': title.strip(),
                                'timestamp': datetime.now().isoformat(),
                                'source': 'orb_history'
                            })
                    except:
                        continue

            print(f"[+] Found {len(tracks)} tracks from {station_url}")
            return tracks

        except Exception as e:
            print(f"[-] Error scraping {station_url}: {e}")
            return tracks

    def save_to_database(self, station_id, tracks):
        """Save scraped tracks to database"""
        if not tracks:
            return 0

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            saved_count = 0

            for track in tracks:
                try:
                    # Insert into DetectionLog
                    cursor.execute('''
                        INSERT INTO "DetectionLog" 
                        (id, stationId, observedAt, detectionMethod, parsedArtist, parsedTitle, 
                         status, sourceProvider, confidence)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        f"orb_backfill_{station_id}_{int(time.time() * 1000)}",
                        station_id,
                        track['timestamp'],
                        'orb_page_scrape',
                        track['artist'],
                        track['title'],
                        'matched',
                        'onlineradiobox.com',
                        0.85
                    ))

                    saved_count += 1

                except sqlite3.IntegrityError:
                    # Track already exists, skip
                    pass

            conn.commit()
            conn.close()

            print(f"[+] Saved {saved_count}/{len(tracks)} tracks for {station_id}")
            return saved_count

        except Exception as e:
            print(f"[-] Database error: {e}")
            return 0

    def backfill_all_zambian_stations(self):
        """Backfill playlists for all Zambian stations"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get all Zambian stations with ORB IDs
            cursor.execute('''
                SELECT id, name FROM Station 
                WHERE country='Zambia' AND isActive=1
                ORDER BY name
            ''')

            stations = cursor.fetchall()
            conn.close()

            print(f"\n[*] Starting backfill for {len(stations)} stations...\n")

            total_tracks = 0
            success_count = 0

            for station_id, station_name in stations:
                station_url = self.get_station_url(station_id, station_name)

                if not station_url:
                    print(f"[!] Skipping {station_name} (no ORB URL)")
                    continue

                print(f"\n--- {station_name} ---")

                tracks = self.scrape_track_history(station_url, station_id)

                if tracks:
                    saved = self.save_to_database(station_id, tracks)
                    total_tracks += saved
                    success_count += 1

                # Rate limiting - be nice to the server
                time.sleep(2)

            print(f"\n[✓] Backfill complete!")
            print(f"    Stations processed: {success_count}/{len(stations)}")
            print(f"    Total tracks added: {total_tracks}")

            return success_count, total_tracks

        except Exception as e:
            print(f"[-] Error: {e}")
            return 0, 0

    def backfill_chikuni_specifically(self):
        """Focus on Chikuni with multiple URLs and approaches"""
        urls = [
            "https://onlineradiobox.com/zm/chikuni/",
            "https://onlineradiobox.com/zm/chikuni/?cs=zm.lusaka977"
        ]

        print("\n=== CHIKUNI RADIO SPECIFIC BACKFILL ===\n")

        total_tracks = 0

        for url in urls:
            print(f"\n[*] Scraping Chikuni from: {url}")
            tracks = self.scrape_track_history(url, "zm_orb_zm.chikuni", max_pages=10)

            if tracks:
                saved = self.save_to_database("zm_orb_zm.chikuni", tracks)
                total_tracks += saved

            time.sleep(3)

        print(f"\n[✓] Chikuni backfill complete: {total_tracks} tracks added")
        return total_tracks

if __name__ == "__main__":
    scraper = ORBPlaylistScraper()

    if len(sys.argv) > 1 and sys.argv[1] == '--chikuni':
        # Backfill Chikuni specifically
        scraper.backfill_chikuni_specifically()
    else:
        # Backfill all stations
        success, total = scraper.backfill_all_zambian_stations()

        if total > 0:
            print(f"\n[✓] SUCCESS: Added {total} tracks to database")
            sys.exit(0)
        else:
            print(f"\n[!] WARNING: No tracks were added")
            sys.exit(1)
