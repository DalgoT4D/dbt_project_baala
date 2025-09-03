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
- **Schema**: `kobotoolbox_source_data`

### 2. Staging Models (`models/staging/`)
- **41 individual staging models** - one for each survey
- **Base model** (`stg_survey_base.sql`) providing common structure
- **JSONB flattening** using custom macros for consistent data extraction
- **Standardized field naming** across all surveys
- **Data quality indicators** and metadata enrichment

## Key Features

### JSONB Flattening
- Custom macros for consistent JSONB field extraction
- Standardized field naming conventions
- Null handling and data type casting

### Dynamic Data Handling
- **Automatic Field Discovery**: Automatically detects and extracts all fields from any survey
- **Flexible Schema**: Adapts to different survey structures without code changes
- **Field Categorization**: Automatically categorizes fields by type (demographics, location, etc.)
- **Schema Analysis**: Provides insights into what fields are available across surveys

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
├── intermediate/                        # Combined survey models (currently empty)
└── marts/                              # Final analysis models (currently empty)
```

## Macros

### `macros/flatten_jsonb.sql`
- `flatten_jsonb_column()` - Extracts common survey fields
- `extract_jsonb_value()` - Safe JSONB value extraction
- `extract_jsonb_array()` - JSONB array extraction
- `extract_jsonb_object()` - JSONB object extraction

### `macros/dynamic_jsonb_extractor.sql` (NEW!)
- `extract_all_jsonb_fields()` - Dynamically extracts all fields from any survey
- `extract_jsonb_keys()` - Discovers all available fields in JSONB data
- `extract_jsonb_field_if_exists()` - Safely extracts fields that may not exist
- `extract_conditional_fields()` - Extracts fields based on conditional logic
- `create_dynamic_schema()` - Generates schema information from JSONB structure

## Usage Examples

### 1. Query Individual Survey Data
```sql
-- Get CAF Ajmer baseline data
select * from {{ ref('stg_caf_ajmer_2024_baselineendline_survey') }}
where survey_phase = 'baseline';
```

### 2. Dynamic Field Discovery
```sql
-- Discover what fields are available in any survey
select 
    survey_name,
    all_field_names,
    field_count,
    has_demographic_fields,
    has_location_fields,
    has_socioeconomic_fields
from {{ ref('stg_survey_structure_analyzer') }}
where survey_name = 'your_survey_name';
```

### 3. Dynamic Field Extraction
```sql
-- Use the dynamic template for any survey
-- This automatically adapts to the survey structure
select * from {{ ref('stg_dynamic_survey_template') }};
```

### 2. Cross-Survey Analysis
```sql
-- Compare baseline vs endline across FBC projects
select 
    'FBC School Boys' as project,
    'Baseline' as phase,
    count(*) as respondent_count,
    avg(try_cast(respondent_age as integer)) as avg_age
from {{ ref('stg_fbc_school_boys_2024_baseline_survey') }}

union all

select 
    'FBC School Boys' as project,
    'Endline' as phase,
    count(*) as respondent_count,
    avg(try_cast(respondent_age as integer)) as avg_age
from {{ ref('stg_fbc_school_boys_2024_endline_survey') }};
```

### 3. Organization-wise Analysis
```sql
-- Get all CAF surveys
select 
    'CAF' as organization,
    count(*) as total_respondents
from {{ ref('stg_caf_ajmer_2024_baselineendline_survey') }}

union all

select 
    'CAF' as organization,
    count(*) as total_respondents
from {{ ref('stg_caf_ajmer_students_2025_baseline_survey') }}

union all

select 
    'CAF' as organization,
    count(*) as total_respondents
from {{ ref('stg_caf_delhi_school_2024_baseline_survey') }}

union all

select 
    'CAF' as organization,
    count(*) as total_respondents
from {{ ref('stg_caf_delhi_school_2025_endline_survey') }};
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

1. **Modular Design**: Separate models for different surveys
2. **Consistent Naming**: Standardized naming conventions across all models
3. **Documentation**: Comprehensive descriptions for all models and fields
4. **Testing**: Built-in data quality checks and indicators
5. **Performance**: Appropriate materialization strategies (views for staging)
6. **Maintainability**: Reusable macros and consistent patterns

## Deployment

### Prerequisites
- dbt installed and configured
- Database connection established
- Proper permissions for table creation
- Access to `kobotoolbox_source_data` schema

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

1. **Data Quality Tests**: Add specific tests for data validation
2. **Incremental Models**: Implement incremental processing for large datasets
3. **Performance Optimization**: Add indexes and partitioning strategies
4. **Additional Staging Models**: Create specialized models for specific analysis needs

## Support

For questions or issues with the survey models:
1. Check the model documentation in dbt docs
2. Review the source definitions in `sources.yml`
3. Examine the staging models for field mappings
4. Use individual staging models for survey-specific analysis

## Contributing

When adding new surveys:
1. Add source definition to `sources.yml` in `kobotoolbox_source_data` schema
2. Create staging model following the established pattern
3. Add proper documentation and tags
4. Update this README if needed
