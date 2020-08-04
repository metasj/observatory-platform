# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Richard Hosking

WITH DOIs as (
SELECT
  *,
  ARRAY(SELECT DISTINCT grid FROM UNNEST( ARRAY_CONCAT( IF(microsoft_academic_graph.grids IS NOT NULL, microsoft_academic_graph.grids, []), IF( wos.grids IS NOT NULL, wos.grids, []), IF(scopus.grids IS NOT NULL, scopus.grids, []))) AS grid ) as grids
FROM
  (SELECT 
    ref.doi as doi,
    STRUCT( title, abstract, issued.date_parts[offset(0)] as published_year, type, ISSN, ISBN, issn_type, publisher_location, publisher, container_title, references_count, alternative_id, subject, published_print, license, volume, funder, is_referenced_by_count, page, author ) as crossref,
    (SELECT as STRUCT * from `academic-observatory-workflow.unpaywall_processed.unpaywall_2019_11_22` as oa WHERE oa.doi = ref.doi) as unpaywall,
    ARRAY((SELECT as STRUCT _TABLE_SUFFIX as unpaywall_release, * from `academic-observatory-workflow.unpaywall_processed.*` as oa WHERE oa.doi = ref.doi)) as unpaywall_history,
    (SELECT as STRUCT * from `academic-observatory-workflow.mag_processed.mag_2019_10_03` as mag WHERE mag.doi = ref.doi) as microsoft_academic_graph,
    (SELECT as STRUCT * from `academic-observatory-workflow.open_citations_processed.open_citations_2018_11_12` as oa WHERE oa.doi = ref.doi) as open_citations,
    (SELECT as STRUCT * from `academic-observatory-workflow.wos_processed.wos_2020_01_26`  as wos WHERE wos.doi = ref.doi) as wos,
    (SELECT as STRUCT * from `academic-observatory-workflow.scopus_processed.scopus_2020_01_26` as scopus WHERE scopus.doi = ref.doi) as scopus,
    (SELECT as STRUCT * from `academic-observatory-workflow.crossref_events_processed.crossref_events_2020_01_26` as events WHERE events.doi = ref.doi) as events
  FROM `academic-observatory-telescope.crossref.crossref_2019_09` as ref
  WHERE ARRAY_LENGTH(issued.date_parts) > 0)
),

affiliations as (
SELECT
  extras.doi as doi,
  extras.crossref.published_year,
  
  institutions,
  
  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Country"] as types, ARRAY_AGG(home_repo IGNORE NULLS) as home_repos, MAX(country) as country, MAX(country_code) as country_code, MAX(country_code_2) as country_code_2, MAX(region) as region, MAX(subregion) as subregion, NULL as coordinates, COUNT(*) as count, COUNTIF(in_scopus) as count_scopus, COUNTIF(in_wos) as count_wos, COUNTIF(in_mag) as count_mag, TRUE in UNNEST(ARRAY_AGG(in_scopus)) as in_scopus, TRUE in UNNEST(ARRAY_AGG(in_wos)) as in_wos, TRUE in UNNEST(ARRAY_AGG(in_mag)) as in_mag FROM UNNEST(countries) GROUP BY identifier) as countries,

  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Subregion"] as types, ARRAY_AGG(home_repo IGNORE NULLS) as home_repos, MAX(country) as country, MAX(country_code) as country_code, MAX(region) as region, MAX(subregion) as subregion, NULL as coordinates, COUNT(*) as count, COUNTIF(in_scopus) as count_scopus, COUNTIF(in_wos) as count_wos, COUNTIF(in_mag) as count_ma, TRUE in UNNEST(ARRAY_AGG(in_scopus)) as in_scopus, TRUE in UNNEST(ARRAY_AGG(in_wos)) as in_wos, TRUE in UNNEST(ARRAY_AGG(in_mag)) as in_mag, FROM UNNEST(subregions) GROUP BY identifier) as subregions,
  
  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Region"] as types, ARRAY_AGG(home_repo IGNORE NULLS) as home_repos, MAX(country) as country, MAX(country_code) as country_code, MAX(region) as region, MAX(subregion) as subregion, NULL as coordinates, COUNT(*) as count, COUNTIF(in_scopus) as count_scopus, COUNTIF(in_wos) as count_wos, COUNTIF(in_mag) as count_mag, TRUE in UNNEST(ARRAY_AGG(in_scopus)) as in_scopus, TRUE in UNNEST(ARRAY_AGG(in_wos)) as in_wos, TRUE in UNNEST(ARRAY_AGG(in_mag)) as in_mag, FROM UNNEST(regions) GROUP BY identifier) as regions,
  
  ARRAY(SELECT as STRUCT identifier, MAX(name) as name, ["Grouping"] as types, ARRAY_AGG(home_repo IGNORE NULLS) as home_repos, MAX(country) as country, MAX(country_code) as country_code, MAX(region) as region, MAX(subregion) as subregion, NULL as coordinates, COUNT(*) as count, COUNTIF(in_scopus) as count_scopus, COUNTIF(in_wos) as count_wos, COUNTIF(in_mag) as count_mag, TRUE in UNNEST(ARRAY_AGG(in_scopus)) as in_scopus, TRUE in UNNEST(ARRAY_AGG(in_wos)) as in_wos, TRUE in UNNEST(ARRAY_AGG(in_mag)) as in_mag, FROM UNNEST(groupings) GROUP BY identifier) as groupings,
  
  -- Funder 
  #ARRAY(SELECT as STRUCT funder.funder.doi as identifier, funder.funder.name as name, ["Funder"] as types, NULL as home_repos, funder.fundref.country as country, funder.fundref.country_code as country_code, funder.fundref.region as region, NULL as subregion, NULL as coordinates, funder.fundref.funding_body_type as funding_body_type, funder.fundref.funding_body_sub_type as funding_body_subtype, scopus.doi is NOT NULL as in_scopus, wos.doi is NOT NULL as in_wos, microsoft_academic_graph.doi is NOT NULL as in_mag, TRUE in UNNEST(in_filtered) as in_filtered FROM UNNEST(fundref.funders) as funder) as funders,
  
  -- Journal  
  [ STRUCT( unpaywall.journal_name as identifier, unpaywall.journal_name as name, ["Journal"] as types, NULL as home_repos, NULL as country, NULL as country_code, NULL as region, NULL as subregion, NULL as coordinates, scopus.doi is NOT NULL as in_scopus, wos.doi is NOT NULL as in_wos, microsoft_academic_graph.doi is NOT NULL as in_mag )] as journals,
  
  -- Publisher 
  [ STRUCT( crossref.publisher as identifier,crossref.publisher as name, ["Publisher"] as types, NULL as home_repos, NULL as country, NULL as country_code, NULL as region, NULL as subregion, NULL as coordinates,scopus.doi is NOT NULL as in_scopus, wos.doi is NOT NULL as in_wos, microsoft_academic_graph.doi is NOT NULL as in_mag) ] as publishers,
  
FROM
  `academic-observatory.doi.dois_2020_02_12` as extras
LEFT JOIN(
SELECT
  doi,
    MAX(crossref.published_year) as published_year,
    #ARRAY_AGG(institution.id in (SELECT id from filtered_list)) as in_filtered,
    
    ARRAY_AGG(
      STRUCT(
        institution.id as identifier,
        institution.types as types,
        institution.name as name,
        institution.home_repo as home_repo,
        institution.iso3166.regions.name as country,
        institution.iso3166.regions.alpha3 as country_code,
        institution.iso3166.regions.region as region,
        institution.iso3166.regions.subregion as subregion,
        CONCAT(CAST(institution.addresses[SAFE_OFFSET(0)].lat as STRING), ", ", CAST(institution.addresses[SAFE_OFFSET(0)].lng as STRING)) as coordinates,
        institution.id in UNNEST(dois.wos.grids) as in_wos,
        institution.id in UNNEST(dois.scopus.grids) as in_scopus,
        institution.id in UNNEST(dois.microsoft_academic_graph.grids) as in_mag
        #institution.id in (SELECT id from filtered_list) as in_filtered
      )
    ) as institutions,

    ARRAY_AGG(
      STRUCT(
        institution.iso3166.regions.alpha3 as identifier,
        institution.types as types,
        institution.iso3166.regions.name as name,
        institution.home_repo as home_repo,
        institution.iso3166.regions.name as country,
        institution.iso3166.regions.alpha3 as country_code,
        institution.iso3166.regions.alpha2 as country_code_2,
        institution.iso3166.regions.region as region,
        institution.iso3166.regions.subregion as subregion,
        NULL as coordinates,
        institution.id in UNNEST(dois.wos.grids) as in_wos,
        institution.id in UNNEST(dois.scopus.grids) as in_scopus,
        institution.id in UNNEST(dois.microsoft_academic_graph.grids) as in_mag,
        #institution.id in (SELECT id from filtered_list) as in_filtered,
        institution.id as institutional_identifier
      )
    ) as countries,
    
    ARRAY_AGG(
      STRUCT(
        institution.iso3166.regions.subregion as identifier,
        NULL as types,
        institution.iso3166.regions.subregion as name,
        institution.home_repo as home_repo,
        NULL as country,
        NULL as country_code,
        institution.iso3166.regions.region as region,
        NULL as subregion,
        NULL as coordinates,
        institution.id in UNNEST(dois.wos.grids) as in_wos,
        institution.id in UNNEST(dois.scopus.grids) as in_scopus,
        institution.id in UNNEST(dois.microsoft_academic_graph.grids) as in_mag,
        #institution.id in (SELECT id from filtered_list) as in_filtered,
        institution.id as institutional_identifier
      )
    ) as subregions,

    ARRAY_AGG(
      STRUCT(
        institution.iso3166.regions.region as identifier,
        NULL as types,
        institution.iso3166.regions.region as name,
        institution.home_repo as home_repo,
        NULL as country,
        NULL as country_code,
        institution.iso3166.regions.region as region,
        NULL as subregion,
        NULL as coordinates,
        institution.id in UNNEST(dois.wos.grids) as in_wos,
        institution.id in UNNEST(dois.scopus.grids) as in_scopus,
        institution.id in UNNEST(dois.microsoft_academic_graph.grids) as in_mag,
        #institution.id in (SELECT id from filtered_list) as in_filtered,
        institution.id as institutional_identifier
      )
    ) as regions,
    
    ARRAY_AGG(
      STRUCT(
        groupings.group_id as identifier,
        NULL as types,
        groupings.group_name as name, 
        NULL as home_repo,
        NULL as country,
        groupings.country_code as country_code, 
        NULL as region,
        NULL as subregion,
        NULL as coordinates,
        institution.id in UNNEST(dois.wos.grids) as in_wos,
        institution.id in UNNEST(dois.scopus.grids) as in_scopus,
        institution.id in UNNEST(dois.microsoft_academic_graph.grids) as in_mag,
        #institution.id in (SELECT id from filtered_list) as in_filtered,
        institution.id as institutional_identifier
      )
    ) as groupings,

  FROM DOIs as dois, UNNEST(grids) as grid_id
  LEFT JOIN `open-knowledge-datasets.intermediate_2019_Oct.grids_with_regions_and_home_repos` as institution on grid_id = institution.id
  LEFT JOIN (SELECT group_id, group_name, country_code, grids FROM `open-knowledge-datasets.mappings.groupings` CROSS JOIN UNNEST(grids) as grids) as groupings on institution.id = groupings.grids
  GROUP BY doi) as base on extras.doi = base.doi
)

SELECT
  dois.*,
  affiliations
FROM DOIs as dois
LEFT JOIN affiliations as affiliations on affiliations.doi = dois.doi