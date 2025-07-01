# Changelog

## Version 1.21.1-2.1.1

### Changes:
- Ported from Fabric to NeoForge 1.21.1
- Updated command system to use NeoForge's command registration
- Updated configuration system to use NeoForge's FMLPaths
- Updated event handling to use NeoForge's event bus system
- Removed Fabric-specific dependencies and replaced with NeoForge equivalents

### Fixes:
- Config will now be created correctly unless the entire folder is deleted
- Server fails to shut down and remains stuck (fixed with proper NeoForge event handling)

### Technical Changes:
- Migrated from Fabric Loader API to NeoForge FML
- Updated build system to use NeoForge Gradle plugin
- Replaced Fabric permissions API usage with NeoForge permission checks
- Updated mod metadata format from fabric.mod.json to neoforge.mods.toml