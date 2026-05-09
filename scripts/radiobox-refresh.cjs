const { PrismaClient } = require('@prisma/client');
const { MetadataService } = require('../dist/server/services/metadata.service.js');

const p = new PrismaClient();

function textOf(m) {
  return (m && (m.combinedRaw || m.rawTitle || m.rawArtist || '') || '').trim();
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

(async () => {
  const started = new Date();
  console.log('\n==== RadioBox refresh started ' + started.toISOString() + ' ====');

  const rows = await p.station.findMany({
    where: {
      isActive: true,
      metadataPriorityEnabled: true
    },
    take: 10000
  });

  const targets = rows.filter(s => /onlineradiobox\.com/i.test(s.sourceIdsJson || ''));

  let checked = 0;
  let updated = 0;
  let nulls = 0;
  let errors = 0;

  for (const st of targets) {
    checked++;

    try {
      const meta = await MetadataService.readOnlineRadioBoxMetadata(st.sourceIdsJson || '', st.name);
      const txt = textOf(meta);

      if (!txt || txt.length < 4) {
        nulls++;
        console.log('ORB_NULL', st.name);
        await sleep(700);
        continue;
      }

      await p.station.update({
        where: { id: st.id },
        data: {
          metadataAvailableLast: 1,
          metadataPresentEma: 1,
          metadataPriorityEnabled: true,
          monitorState: 'ACTIVE',
          monitorStateReason: 'metadata-priority-online-radiobox-icy',
          lastPollStatus: 'ok',
          lastPollError: null,
          lastMetadataAt: new Date(),
          lastSongDetectedAt: new Date(),
          songIdentifiedLast: 1,
          lastGoodAudioAt: new Date()
        }
      });

      updated++;
      console.log('ORB_UPDATED', st.name, '=>', txt.slice(0, 140));
    } catch (e) {
      errors++;
      console.log('ORB_ERROR', st.name, String(e.message || e).slice(0, 180));
    }

    await sleep(700);
  }

  console.log('SUMMARY checked=' + checked + ' updated=' + updated + ' nulls=' + nulls + ' errors=' + errors);
  console.log('==== RadioBox refresh finished ' + new Date().toISOString() + ' ====');
})()
.catch(e => {
  console.error('RADIOBOX_REFRESH_FATAL', e);
  process.exit(1);
})
.finally(() => p.$disconnect());
