from __future__ import annotations

from dataclasses import dataclass

from pathlib import Path

@dataclass(frozen=True, slots=True)
class Profile:
    id: str
    ...


def infer_profile(path: Path) -> Profile:
    _ = path
    return Profile("TODO")

def default_profile() -> Profile:
    return Profile("TODO")

# Core filetypes (≈95% coverage)
#     YAML (.yml, .yaml) — typed
#     JSON (.json) — typed
#     TOML (.toml) — typed
#     XML (.xml) — attributes must be quoted; element text is free-form
#     Properties / INI / dotenv key=value (.ini, .conf, .properties, .env) — stringly
#     Shell & scheduler scripts (Bash/Slurm/PBS) (.sh, .sbatch, .pbs) — stringly
#     Fortran NAMELIST (.nml, .namelist) — typed (.true./.false. booleans)
#
# Future (not right now)
#     Domain “input decks”:
#        * LAMMPS (.in/.lmp),
#        * GROMACS (.mdp),
#        * VASP (INCAR/KPOINTS/POSCAR),
#        * CP2K (.inp),
#        * Gaussian/ORCA (.gjf/.com/.inp),
#        * NAMD/CHARMM (.conf/.namd),
#        *  OpenFOAM dictionaries
#     Make/CMake (Makefile, CMakeLists.txt)
#     CSV/TSV (.csv, .tsv)
#     JSON5 / HJSON
#     Lua/Tcl config scripts
#     Java/Kotlin .properties if not already covered by the “properties/ini” profile variant
#
# Profile specification (what each filetype “profile” should define)
#     id/name: short identifier for the profile
#     extensions: list of filename extensions it applies to
#     category: one of {typed, stringly, xml}
#     placeholder_quoting:
#         require_full_quoted_placeholder: {always, when_in_value, never}
#         default_outer_quote: {double, single, none} (how users should write "{{ TL ... }}")
#     smart_unquoting (typed formats):
#         enabled: bool (drop surrounding quotes when placeholder resolves to non-string)
#         drop_for_types: subset of {number, boolean, null}
#     boolean_literals: mapping of true/false representations
#         * (e.g., JSON: true/false, Fortran: .true./.false.)
#     null_literal: representation of null/none (e.g., null, None, nil, empty string policy)
#     string_escaping:
#         escape_mode: {json, yaml, toml, xml, shell, fortran, tcl, none}
#         multiline_policy: {escape_newlines, native_block_if_supported, forbid}
#     numeric_formatting:
#         pass_through (default), optional tweaks (e.g., Fortran exponent style E/D if ever needed)
#     xml_specific (for xml category):
#         attributes_must_remain_quoted: bool (true)
#         element_text_rules: whether unquoted placeholders are allowed in element text (true)
#     shell_like_specific (for stringly shells/schedulers):
#         never_unquote: bool (keep quotes even for numbers)
#         argv_boundary_hint: note that quoting affects word splitting; be conservative
#     comments_and_delimiters (for ini/properties/dotenv):
#         comment_prefixes: e.g., # and ;
#         key_value_delimiters: e.g., = and :
#     encoding: default expected encoding for templates (UTF-8; informative)
#     full_quoted_token_detection:
#         regex/pattern used to detect “placeholder fully occupies a quoted scalar”
