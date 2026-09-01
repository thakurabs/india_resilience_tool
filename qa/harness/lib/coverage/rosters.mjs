// Local canonical roster extraction for the data-coverage harness.

import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { checkRosters, resolveDataDir } from './preflight.mjs';
import { writeCsv } from './io.mjs';

const DISTRICT_ALIASES = {
  state: ['state_name', 'st_nm', 'state', 'state_nm', 'adm1_name', 'STATE_NAME'],
  district: ['district_name', 'district', 'district_nm', 'dist_name', 'dtname', 'adm2_name', 'DISTRICT'],
  districtCode: ['district_lgd_code', 'dist_lgd_code', 'district_code', 'dtcode', 'lgd_district_code'],
  areaKm2: ['area_km2', 'area_sqkm', 'area_sq_km'],
};

const BLOCK_ALIASES = {
  state: ['state_name', 'st_nm', 'state', 'state_nm', 'adm1_name', 'STATE_NAME'],
  district: ['district_name', 'district', 'district_nm', 'dist_name', 'dtname', 'adm2_name', 'DISTRICT'],
  block: ['block_name', 'block', 'block_nm', 'subdistrict_name', 'sub_district_name', 'adm3_name'],
  blockCode: ['block_lgd_code', 'block_code', 'subdistrict_lgd_code', 'subdist_lgd_code', 'lgd_block_code'],
  blockKey: ['block_key', 'canonical_block_key', 'unit_key', 'admin_key'],
  districtCode: ['dist_lgd_code', 'district_lgd_code', 'district_code', 'dtcode', 'lgd_district_code'],
  stateCode: ['state_lgd_code', 'state_code', 'lgd_state_code'],
};

function normalize(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

function readGeoJson(path) {
  const parsed = JSON.parse(readFileSync(path, 'utf8'));
  if (!Array.isArray(parsed.features)) {
    throw new Error(`Expected FeatureCollection with features array: ${path}`);
  }
  return parsed.features;
}

function firstProperties(features, path) {
  const props = features.find((feature) => feature && feature.properties)?.properties;
  if (!props) throw new Error(`No feature properties found in ${path}`);
  return props;
}

function resolveField(props, aliases, { required = true, logicalName }) {
  const keys = new Set(Object.keys(props));
  const found = aliases.find((name) => keys.has(name));
  if (!found && required) {
    throw new Error(`Could not resolve required field "${logicalName}". Tried: ${aliases.join(', ')}`);
  }
  return found || null;
}

function uniqueSorted(values) {
  return [...new Set(values.map(normalize).filter(Boolean))].sort((a, b) => a.localeCompare(b));
}

function buildDistrictRows(features, fields) {
  return features.map((feature, idx) => {
    const props = feature.properties || {};
    const stateName = normalize(props[fields.state]);
    const districtName = normalize(props[fields.district]);
    if (!stateName || !districtName) {
      throw new Error(`District feature ${idx} has empty state or district identity`);
    }
    return {
      state_name: stateName,
      district_name: districtName,
      district_lgd_code: fields.districtCode ? normalize(props[fields.districtCode]) : '',
      area_km2: fields.areaKm2 ? props[fields.areaKm2] ?? '' : '',
    };
  }).sort((a, b) => (
    a.state_name.localeCompare(b.state_name)
    || a.district_name.localeCompare(b.district_name)
    || String(a.district_lgd_code).localeCompare(String(b.district_lgd_code))
  ));
}

function buildBlockRows(features, fields) {
  return features.map((feature, idx) => {
    const props = feature.properties || {};
    const stateName = normalize(props[fields.state]);
    const districtName = normalize(props[fields.district]);
    const blockName = normalize(props[fields.block]);
    if (!stateName || !districtName || !blockName) {
      throw new Error(`Block feature ${idx} has empty state, district, or block identity`);
    }
    const blockKey = fields.blockKey
      ? normalize(props[fields.blockKey])
      : `${stateName}::${districtName}::${blockName}`;
    return {
      state_name: stateName,
      district_name: districtName,
      block_name: blockName,
      block_lgd_code: fields.blockCode ? normalize(props[fields.blockCode]) : '',
      block_key: blockKey,
      district_lgd_code: fields.districtCode ? normalize(props[fields.districtCode]) : '',
      state_lgd_code: fields.stateCode ? normalize(props[fields.stateCode]) : '',
    };
  }).sort((a, b) => (
    a.state_name.localeCompare(b.state_name)
    || a.district_name.localeCompare(b.district_name)
    || a.block_name.localeCompare(b.block_name)
    || String(a.block_lgd_code).localeCompare(String(b.block_lgd_code))
  ));
}

function duplicateKeys(rows, keyFields) {
  const counts = new Map();
  for (const row of rows) {
    const key = keyFields.map((field) => normalize(row[field])).join('|');
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return [...counts.entries()]
    .filter(([, count]) => count > 1)
    .map(([key, count]) => ({ key, count }));
}

function countsByState(districtRows, blockRows) {
  const states = uniqueSorted([...districtRows.map((r) => r.state_name), ...blockRows.map((r) => r.state_name)]);
  return states.flatMap((stateName) => [
    {
      state_name: stateName,
      admin_level: 'district',
      expected_count: districtRows.filter((row) => row.state_name === stateName).length,
    },
    {
      state_name: stateName,
      admin_level: 'block',
      expected_count: blockRows.filter((row) => row.state_name === stateName).length,
    },
  ]);
}

/** Build and write Phase 1 roster outputs. */
export function writeExpectedRosters(runDir) {
  const rosterPreflight = checkRosters();
  if (!rosterPreflight.ok) {
    throw new Error(`Canonical roster files are missing under ${rosterPreflight.dataDir || '<unresolved IRT_DATA_DIR>'}`);
  }
  const dataDir = resolveDataDir();
  const districtPath = join(dataDir, 'districts_4326.geojson');
  const blockPath = join(dataDir, 'blocks_4326.geojson');
  const districtFeatures = readGeoJson(districtPath);
  const blockFeatures = readGeoJson(blockPath);
  const districtProps = firstProperties(districtFeatures, districtPath);
  const blockProps = firstProperties(blockFeatures, blockPath);

  const districtFields = {
    state: resolveField(districtProps, DISTRICT_ALIASES.state, { logicalName: 'district.state' }),
    district: resolveField(districtProps, DISTRICT_ALIASES.district, { logicalName: 'district.district' }),
    districtCode: resolveField(districtProps, DISTRICT_ALIASES.districtCode, { logicalName: 'district.districtCode', required: false }),
    areaKm2: resolveField(districtProps, DISTRICT_ALIASES.areaKm2, { logicalName: 'district.areaKm2', required: false }),
  };
  const blockFields = {
    state: resolveField(blockProps, BLOCK_ALIASES.state, { logicalName: 'block.state' }),
    district: resolveField(blockProps, BLOCK_ALIASES.district, { logicalName: 'block.district' }),
    block: resolveField(blockProps, BLOCK_ALIASES.block, { logicalName: 'block.block' }),
    blockCode: resolveField(blockProps, BLOCK_ALIASES.blockCode, { logicalName: 'block.blockCode', required: false }),
    blockKey: resolveField(blockProps, BLOCK_ALIASES.blockKey, { logicalName: 'block.blockKey', required: false }),
    districtCode: resolveField(blockProps, BLOCK_ALIASES.districtCode, { logicalName: 'block.districtCode', required: false }),
    stateCode: resolveField(blockProps, BLOCK_ALIASES.stateCode, { logicalName: 'block.stateCode', required: false }),
  };

  const districtRows = buildDistrictRows(districtFeatures, districtFields);
  const blockRows = buildBlockRows(blockFeatures, blockFields);
  const stateRows = uniqueSorted([...districtRows.map((r) => r.state_name), ...blockRows.map((r) => r.state_name)])
    .map((stateName) => ({ state_name: stateName }));
  const countRows = countsByState(districtRows, blockRows);

  writeCsv(join(runDir, 'expected_states.csv'), stateRows, ['state_name']);
  writeCsv(join(runDir, 'expected_districts.csv'), districtRows, ['state_name', 'district_name', 'district_lgd_code', 'area_km2']);
  writeCsv(join(runDir, 'expected_blocks.csv'), blockRows, ['state_name', 'district_name', 'block_name', 'block_lgd_code', 'block_key', 'district_lgd_code', 'state_lgd_code']);
  writeCsv(join(runDir, 'expected_counts_by_state_level.csv'), countRows, ['state_name', 'admin_level', 'expected_count']);

  const summary = {
    timestamp: new Date().toISOString(),
    dataDir,
    sourceFiles: {
      districts: districtPath,
      blocks: blockPath,
    },
    resolvedFields: {
      district: districtFields,
      block: blockFields,
    },
    totals: {
      states: stateRows.length,
      districts: districtRows.length,
      blocks: blockRows.length,
    },
    duplicateChecks: {
      districtsByStateDistrict: duplicateKeys(districtRows, ['state_name', 'district_name']),
      blocksByBlockKey: duplicateKeys(blockRows, ['block_key']),
      blocksByStateDistrictBlock: duplicateKeys(blockRows, ['state_name', 'district_name', 'block_name']),
    },
    outputs: {
      expectedStates: join(runDir, 'expected_states.csv'),
      expectedDistricts: join(runDir, 'expected_districts.csv'),
      expectedBlocks: join(runDir, 'expected_blocks.csv'),
      expectedCountsByStateLevel: join(runDir, 'expected_counts_by_state_level.csv'),
    },
  };
  writeFileSync(join(runDir, 'expected_roster_summary.json'), JSON.stringify(summary, null, 2));
  return summary;
}
