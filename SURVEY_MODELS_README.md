# Survey Data Models - dbt Project

This dbt project provides a comprehensive data transformation pipeline for survey data from various development projects and organizations. The project follows dbt best practices and creates a structured, analyzable dataset from raw JSONB survey responses.

## Project Overview

The project processes survey data from **41 different surveys** across multiple organizations including:
- **CAF (Child Aid Foundation)** - Education and community development projects
- **FBC (Foundation for Better Childhood)** - Childhood development and women empowerment
- **MSI** - Community development and training programs
- **MAMTA** - School-based interventions and workshops
- **NTPC** - Community development projects
- And many more individual projects

## Data Architecture

### 1. Sources (`models/sources.yml`)
- **41 survey tables** defined as sources
- Each source includes proper descriptions and metadata
- Standardized naming conventions for easy reference

### 2. Staging Models (`models/staging/`)
- **41 individual staging models** - one for each survey
- **Base model** (`stg_survey_base.sql`) providing common structure
- **JSONB flattening** using custom macros for consistent data extraction
- **Standardized field naming** across all surveys
- **Data quality indicators** and metadata enrichment

### 3. Intermediate Models (`models/intermediate/`)
- **Combined models** for related surveys (e.g., `int_caf_surveys.sql`)
- **Data enrichment** with project categorization and standardization
- **Cross-survey analysis** capabilities

### 4. Mart Models (`models/marts/`)
- **Comprehensive overview** (`mart_survey_overview.sql`) combining all surveys
- **Standardized categories** for analysis and reporting
- **Unified data structure** for business intelligence

## Key Features

### JSONB Flattening
- Custom macros for consistent JSONB field extraction
- Standardized field naming conventions
- Null handling and data type casting

### Data Standardization
- Common respondent demographics (age, gender, location, education)
- Project categorization and organization grouping
- Survey phase identification (baseline/endline)
- Location standardization

### Data Quality
- Metadata tracking (submission time, extraction time)
- Data completeness indicators
- Standardized error handling

## File Structure

```
models/
├── sources.yml                           # Source definitions
├── staging/                             # Individual survey models
│   ├── stg_survey_base.sql             # Base model template
│   ├── stg_caf_ajmer_2024_baselineendline_survey.sql
│   ├── stg_fbc_school_boys_2024_baseline.sql
│   └── ... (39 more staging models)
├── intermediate/                        # Combined survey models
│   ├── int_caf_surveys.sql             # CAF surveys combined
│   └── int_fbc_surveys.sql             # FBC surveys combined
└── marts/                              # Final analysis models
    └── mart_survey_overview.sql        # Comprehensive survey overview
```

## Macros

### `macros/flatten_jsonb.sql`
- `flatten_jsonb_column()` - Extracts common survey fields
- `extract_jsonb_value()` - Safe JSONB value extraction
- `extract_jsonb_array()` - JSONB array extraction
- `extract_jsonb_object()` - JSONB object extraction

## Usage Examples

### 1. Query Individual Survey Data
```sql
-- Get CAF Ajmer baseline data
select * from {{ ref('stg_caf_ajmer_2024_baselineendline_survey') }}
where survey_phase = 'baseline';
```

### 2. Cross-Survey Analysis
```sql
-- Compare baseline vs endline across FBC projects
select 
    target_group,
    survey_phase,
    count(*) as respondent_count,
    avg(try_cast(respondent_age as integer)) as avg_age
from {{ ref('int_fbc_surveys') }}
group by target_group, survey_phase;
```

### 3. Comprehensive Overview
```sql
-- Get all survey data with standardized categories
select 
    organization,
    project_category,
    respondent_category,
    age_group,
    standardized_education,
    count(*) as total_respondents
from {{ ref('mart_survey_overview') }}
group by organization, project_category, respondent_category, age_group, standardized_education;
```

## Data Fields

### Standard Fields (All Surveys)
- **Metadata**: `_id`, `_submission_time`, `_airbyte_extracted_at`
- **Demographics**: `respondent_name`, `respondent_age`, `respondent_gender`
- **Location**: `village`, `district`, `state`
- **Socioeconomic**: `education_level`, `occupation`, `income_level`, `family_size`
- **Project**: `project_name`, `intervention_type`

### Survey-Specific Fields
- **School Surveys**: `school_name`, `class_level`, `teacher_name`, `academic_performance`
- **Community Surveys**: `community_needs`, `resource_availability`, `intervention_recommendations`
- **Workshop Surveys**: `workshop_name`, `duration`, `learning_outcomes`
- **Observation Surveys**: `observation_date`, `checklist_items`, `findings`

## Data Quality

### Completeness Indicators
- `has_json_data` - Whether JSONB data exists
- `has_submission_time` - Whether submission timestamp exists
- `has_end_time` - Whether end timestamp exists

### Standardization
- Age groups: Under_18, 18_25, 26_35, 36_50, Over_50
- Education levels: Primary, Secondary, Higher, University, None
- Income levels: Low, Medium, High
- Project categories: Education_Development, Childhood_Development, Other_Development

## Best Practices Implemented

1. **Modular Design**: Separate models for different concerns
2. **Consistent Naming**: Standardized naming conventions across all models
3. **Documentation**: Comprehensive descriptions for all models and fields
4. **Testing**: Built-in data quality checks and indicators
5. **Performance**: Appropriate materialization strategies (views for staging, tables for marts)
6. **Maintainability**: Reusable macros and consistent patterns

## Deployment

### Prerequisites
- dbt installed and configured
- Database connection established
- Proper permissions for table creation

### Commands
```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --select staging

# Test models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Future Enhancements

1. **Additional Intermediate Models**: Create more combined models for related surveys
2. **Data Quality Tests**: Add specific tests for data validation
3. **Incremental Models**: Implement incremental processing for large datasets
4. **Performance Optimization**: Add indexes and partitioning strategies
5. **Additional Mart Models**: Create specialized models for specific analysis needs

## Support

For questions or issues with the survey models:
1. Check the model documentation in dbt docs
2. Review the source definitions in `sources.yml`
3. Examine the staging models for field mappings
4. Use the comprehensive overview model for cross-survey analysis

## Contributing

When adding new surveys:
1. Add source definition to `sources.yml`
2. Create staging model following the established pattern
3. Update intermediate models if applicable
4. Update the comprehensive overview model
5. Add proper documentation and tags
