# Creating Releases

This guide explains how to create releases with prebuilt binaries using GitHub Actions.

## Automatic Release Process

The GitHub Actions workflow automatically builds cross-compiled binaries for both ARM architectures when you create a version tag.

### Creating a Release

1. **Update version in code** (if applicable)

2. **Commit and push changes:**
   ```bash
   git add .
   git commit -m "Release v1.0.0"
   git push origin master
   ```

3. **Create and push a version tag:**
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

4. **GitHub Actions will automatically:**
   - Build binaries for `armv7` (32-bit) and `aarch64` (64-bit)
   - Create tarballs: `sentinel_rtp_cam-1.0.0-armv7.tar.gz` and `sentinel_rtp_cam-1.0.0-aarch64.tar.gz`
   - Generate SHA256 checksums
   - Create a GitHub Release with all artifacts attached

5. **Monitor the build:**
   - Go to Actions tab: https://github.com/kaszperek/sentinel-video-receiver/actions
   - Watch the "Build and Release" workflow

## Manual Workflow Trigger

You can also manually trigger a build without creating a release:

1. Go to Actions → Build and Release
2. Click "Run workflow"
3. Select branch and click "Run workflow"

This will build the binaries but won't create a release (since no tag was pushed).

## Installation Using GitHub Releases

Once a release is published, users can install using:

```bash
# Install latest version (default)
sudo ./install.sh

# Install specific version
sudo SENTINEL_VERSION=1.0.0 ./install.sh

# Update to latest
sudo ./update.sh

# Update to specific version
sudo SENTINEL_VERSION=1.0.0 ./update.sh
```

## Release Artifacts

Each release includes:
- `sentinel_rtp_cam-{version}-armv7.tar.gz` - 32-bit ARM binary
- `sentinel_rtp_cam-{version}-armv7.tar.gz.sha256` - Checksum
- `sentinel_rtp_cam-{version}-aarch64.tar.gz` - 64-bit ARM binary
- `sentinel_rtp_cam-{version}-aarch64.tar.gz.sha256` - Checksum

## Version Numbering

Follow semantic versioning:
- `v1.0.0` - Major release
- `v1.1.0` - Minor release (new features)
- `v1.0.1` - Patch release (bug fixes)

## Troubleshooting

### Build fails

Check the Actions logs:
1. Go to Actions tab
2. Click on the failed workflow run
3. Expand the failed step to see error details

Common issues:
- **Compilation errors**: Fix code and create a new tag
- **Missing dependencies**: Update the workflow to install required packages

### Release not created

- Ensure you pushed a tag starting with `v` (e.g., `v1.0.0`)
- Check that `GITHUB_TOKEN` has proper permissions (it should by default)

### Binaries not working on Raspberry Pi

- Verify correct architecture selection in workflow
- Check that cross-compilation toolchain is properly configured
- Test locally using Docker cross-compilation before releasing

## Deleting a Release

If you need to delete a bad release:

1. **Delete the release:**
   - Go to Releases → Click on release → Delete release

2. **Delete the tag:**
   ```bash
   git tag -d v1.0.0
   git push --delete origin v1.0.0
   ```

3. **Create new tag with same version** (if needed):
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

## Testing Before Release

Before creating an official release, test the build locally:

```bash
# Build for specific architecture
cd sentinel_rtp_cam
cargo build --release --target aarch64-unknown-linux-gnu --bin app

# Or use the workflow manually without creating a tag
```
