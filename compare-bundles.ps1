param(
  [Parameter(Position = 0)]
  [int]$Iterations = 50
)

$ErrorActionPreference = "Stop"

function Get-FileSizeBytes([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "File not found: $Path"
  }
  return (Get-Item -LiteralPath $Path).Length
}

function Format-KB([double]$Bytes, [int]$Decimals = 2) {
  return ([math]::Round(($Bytes / 1024.0), $Decimals)).ToString("F$Decimals")
}

function Get-MedianMs([string]$ResultsPath) {
  $line = Select-String -LiteralPath $ResultsPath -Pattern "Median:\s+([0-9.]+)\s+ms" | Select-Object -First 1
  if (-not $line) {
    throw "Could not find a 'Median: <n> ms' line in $ResultsPath"
  }
  return [double]$line.Matches[0].Groups[1].Value
}

$tmpDir = [System.IO.Path]::GetTempPath()
$minifiedResults = Join-Path $tmpDir "minified-results.txt"
$unminifiedResults = Join-Path $tmpDir "unminified-results.txt"

Write-Host "================================"
Write-Host "Bundle Load Performance Comparison"
Write-Host "Iterations: $Iterations"
Write-Host "================================"
Write-Host ""

# Build minified
Write-Host "Building minified bundle..."
pnpm build:bundle *> $null

$minifiedSize = Get-FileSizeBytes "dist/index.mjs"
Copy-Item -LiteralPath "dist/index.mjs" -Destination "dist/index.minified.mjs" -Force

Write-Host ("Minified size: {0} KB" -f (Format-KB $minifiedSize 2))
Write-Host ""

# Benchmark minified
Write-Host "Benchmarking minified bundle..."
node benchmark-bundle-fast.mjs $Iterations | Tee-Object -FilePath $minifiedResults
Write-Host ""

# Build unminified
Write-Host "Building unminified bundle..."
$prevSkipMinify = $null
if (Test-Path Env:SKIP_MINIFY) { $prevSkipMinify = $env:SKIP_MINIFY }
$env:SKIP_MINIFY = "1"
try {
  pnpm build:bundle *> $null
} finally {
  if ($null -ne $prevSkipMinify) { $env:SKIP_MINIFY = $prevSkipMinify } else { Remove-Item Env:SKIP_MINIFY -ErrorAction SilentlyContinue }
}

$unminifiedSize = Get-FileSizeBytes "dist/index.mjs"
Copy-Item -LiteralPath "dist/index.mjs" -Destination "dist/index.unminified.mjs" -Force

Write-Host ("Unminified size: {0} KB" -f (Format-KB $unminifiedSize 2))
Write-Host ""

# Benchmark unminified
Write-Host "Benchmarking unminified bundle..."
node benchmark-bundle-fast.mjs $Iterations | Tee-Object -FilePath $unminifiedResults
Write-Host ""

# Extract median times for comparison
$minifiedMedian = Get-MedianMs $minifiedResults
$unminifiedMedian = Get-MedianMs $unminifiedResults

Write-Host "================================"
Write-Host "COMPARISON"
Write-Host "================================"

$sizeDiffBytes = $unminifiedSize - $minifiedSize
$sizeDiffKb = [math]::Round(($sizeDiffBytes / 1024.0), 2)
$sizePctLarger = [math]::Round((($unminifiedSize * 100.0 / $minifiedSize) - 100.0), 1)

Write-Host ("Size difference: {0} KB ({1}% larger)" -f $sizeDiffKb.ToString("F2"), $sizePctLarger.ToString("F1"))
Write-Host ""

if ($minifiedMedian -lt $unminifiedMedian) {
  $diff = [math]::Round(($unminifiedMedian - $minifiedMedian), 3)
  $pct = [math]::Round((($unminifiedMedian - $minifiedMedian) * 100.0 / $minifiedMedian), 1)
  Write-Host ("[OK] Minified is FASTER by {0} ms ({1}% improvement)" -f $diff.ToString("F3"), $pct.ToString("F1"))
} else {
  $diff = [math]::Round(($minifiedMedian - $unminifiedMedian), 3)
  $pct = [math]::Round((($minifiedMedian - $unminifiedMedian) * 100.0 / $unminifiedMedian), 1)
  Write-Host ("[WARN] Unminified is faster by {0} ms ({1}% improvement)" -f $diff.ToString("F3"), $pct.ToString("F1"))
  Write-Host ""
  Write-Host "This suggests V8 parse/compile time dominates over network transfer."
}

# Restore minified version
if (Test-Path -LiteralPath "dist/index.minified.mjs") {
  Move-Item -LiteralPath "dist/index.minified.mjs" -Destination "dist/index.mjs" -Force
}

Write-Host ""
Write-Host ("Results saved in {0} and {1}" -f $minifiedResults, $unminifiedResults)
if (Test-Path -LiteralPath "dist/index.mjs") {
  Write-Host "Restored minified bundle to dist/index.mjs"
}


