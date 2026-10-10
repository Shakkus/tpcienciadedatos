param(
    [string]$InputPath = (Join-Path $PSScriptRoot 'part-r-00000'),
    [string]$OutputPath = (Join-Path $PSScriptRoot 'top20.txt')
)

$stopWords = @('la', 'no', 'en', 'al', 'de', 'el', 'los', 'las', 'y', 'un', 'una', 'the', 'a', 'con')
$entityWords = @('rider', 'scooter', 'vehiculo', 'vehicle')
$counts = @{}

foreach ($line in Get-Content -LiteralPath $InputPath -Encoding utf8) {
    if ($line -notmatch '^(.+?)\s+(\d+)$') {
        throw "Formato de salida WordCount no reconocido: $line"
    }

    $term = $Matches[1].TrimStart('#').ToLowerInvariant()
    $count = [int]$Matches[2]

    if ($term -match '^(st|r|v)\d+$' -or $stopWords -contains $term -or $entityWords -contains $term) {
        continue
    }

    if ($counts.ContainsKey($term)) {
        $counts[$term] += $count
    }
    else {
        $counts[$term] = $count
    }
}

$topTerms = @(
    $counts.GetEnumerator() |
        Sort-Object @{ Expression = 'Value'; Descending = $true }, @{ Expression = 'Key'; Descending = $false } |
        Select-Object -First 20
)

if ($topTerms.Count -lt 20) {
    throw "Se encontraron solo $($topTerms.Count) terminos; se esperaban 20."
}

$lines = @(
    '# Top 20 terminos operativos derivados del output Hadoop WordCount.'
    '# Normalizacion: minusculas; se quita # de hashtags y se suman variantes.'
    '# Se excluyen palabras funcionales, IDs ST/R/V y nombres genericos de entidad.'
    "termino`tfrecuencia"
)
$lines += $topTerms | ForEach-Object { "$($_.Key)`t$($_.Value)" }

$encoding = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($OutputPath, (($lines -join "`n") + "`n"), $encoding)
Get-Content -LiteralPath $OutputPath -Encoding utf8
