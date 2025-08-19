# TaskLattice

## TODO
- [x] Setup basic grammar/parsing with lark 
- [x] Make SourceContext class to hold span bounds and str origin
- [x] Flush out test_parse.py
- [ ] resolve.py & test_resolve.py (ParamResolved)
- [ ] Sort out neovim keybinds (move all of them to same file: keymaps.lua)
- [ ] Make custom error class that wraps lark errors for placeholder parsing, with 'rich' library printing

## Notes/Ideas

### Core Features
- [ ] Parameter Sweeps: Lists, Zips, Products, ...?
- [ ] Run locally or on server with hub nodes and worker node(s)
- [ ] Web interface (based on Plotly Dash)
    - [ ] View current active/queued jobs
    - [ ] View status of individual jobs (memory/cpu vs time, submission time, status, etc)
    - [ ] Allow user to cancel jobs or restart failed jobs
- [ ] Generic results post-processing utilities (mostly just looping over results directories)

### Potential Features
- [] generic optimzation routine on top of TaskLattice basics?

