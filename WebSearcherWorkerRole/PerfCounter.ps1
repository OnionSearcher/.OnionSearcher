$categoryName = "WebSearcherWorker"
$categoryHelp = "Web Searcher Category"
$categoryType = [System.Diagnostics.PerformanceCounterCategoryType]::SingleInstance

#$categoryExists = [System.Diagnostics.PerformanceCounterCategory]::Exists($categoryName)
#If ($categoryExists)
#{
#	[System.Diagnostics.PerformanceCounterCategory]::Delete($categoryName)
#}

$categoryExists = [System.Diagnostics.PerformanceCounterCategory]::Exists($categoryName)
If (-Not $categoryExists)
{
  $objCCDC = New-Object System.Diagnostics.CounterCreationDataCollection
 
  $objCCD4 = New-Object System.Diagnostics.CounterCreationData
  $objCCD4.CounterName = "CrawleStarted"
  $objCCD4.CounterType = "NumberOfItems32"
  $objCCD4.CounterHelp = "Web Searcher Crawle Started"
  $objCCDC.Add($objCCD4) | Out-Null
 
  $objCCD7 = New-Object System.Diagnostics.CounterCreationData
  $objCCD7.CounterName = "CrawleValided"
  $objCCD7.CounterType = "NumberOfItems32"
  $objCCD7.CounterHelp = "Web Searcher Crawle Valided"
  $objCCDC.Add($objCCD7) | Out-Null

  [System.Diagnostics.PerformanceCounterCategory]::Create($categoryName, $categoryHelp, $categoryType, $objCCDC) | Out-Null
}
