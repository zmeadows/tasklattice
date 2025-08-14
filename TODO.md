# TaskLattice

## TODO
- [x] Setup basic grammar/parsing with lark 
- [x] Make SourceContext class to hold span bounds and str origin
- [ ] Make custom error class that wraps lark errors for placeholder parsing, with 'rich' library printing
- [ ] Flush out test_parse.py
- [ ] ParamResolved
- [ ] validate.py
- [ ] test_validate.py

## Notes/Ideas

Features Supported:
- [ ] YAML Input with custom Tags for sweeps: !var { id: growth_rate, default: 1.2, type: float, ... }
    - [ ] Use ruamel library
    - [ ] Have easy way to use defaults into simulation stand-alone: mysim $(stratasim -d input.yaml)
- [ ] Parameter Sweeps: Lists, Zips, Products, ...?
- [ ] Run locally or on server with hub nodes and worker node(s)
- [ ] Web interface (based on Plotly Dash)
    - [ ] View current active/queued jobs
    - [ ] View status of individual jobs (memory/cpu vs time, submission time, status, etc)
    - [ ] Allow user to cancel jobs or restart failed jobs
- [ ] Generic results post-processing utilities (mostly just looping over results directories)

name for parameter variation container: SimConfigLattice
name for YAML tag: !var
name for run config: SimConfig

for specifying valid bounds, use "domain:" keyword in !var block. Specify closed/open intervals as normal, but explicit sets using curly brackets.
enforce quotes surround 'domain:' entry to avoid confusing parsers/editors for YAML

yaml parameter types allowed: int, float, str, bool, list, null (for optional parameters)

error if no !var entry found in yaml file for var ID given in StrataSim script.

Defaults are not manditory, but stratasim -d should fail if no defaults for any field.

default types should be inferred

use round trip ruamel parsing

integrate ruff + MYPY from the start

COOL IDEA: generic optimzation routine on top of TaskLattice basics?

## For the Simulation itself

Remember you can use YAML anchors/alias/merge to mimick inheritance/defaults

parameter_name: !var
  id: growth_rate           # required (used for sweep ID)
  default: 1.2              # required
  type: float               # optional
  domain: "[0.5, 2.0]"      # optional
  description: "rate of plant growth in cm/day"  # optional
