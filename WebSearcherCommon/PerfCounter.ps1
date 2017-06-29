$categoryName = "WebSearcher"
$categoryHelp = "Web Searcher Category"
$categoryType = [System.Diagnostics.PerformanceCounterCategoryType]::SingleInstance

# If you need to delete the performance object and have it re-created call this: 
#[System.Diagnostics.PerformanceCounterCategory]::Delete($categoryName)

$categoryExists = [System.Diagnostics.PerformanceCounterCategory]::Exists($categoryName)
If (-Not $categoryExists)
{
  $objCCDC = New-Object System.Diagnostics.CounterCreationDataCollection
 
  #$objCCD1 = New-Object System.Diagnostics.CounterCreationData
  #$objCCD1.CounterName = "RoleStarted"
  #$objCCD1.CounterType = "NumberOfItems32"
  #$objCCD1.CounterHelp = "Web Searcher Role Started"
  #$objCCDC.Add($objCCD1) | Out-Null
 
  $objCCD2 = New-Object System.Diagnostics.CounterCreationData
  $objCCD2.CounterName = "CrawleStarted"
  $objCCD2.CounterType = "NumberOfItems32"
  $objCCD2.CounterHelp = "Web Searcher Crawle Started"
  $objCCDC.Add($objCCD2) | Out-Null
 
  $objCCD3 = New-Object System.Diagnostics.CounterCreationData
  $objCCD3.CounterName = "CrawleValided"
  $objCCD3.CounterType = "NumberOfItems32"
  $objCCD3.CounterHelp = "Web Searcher Crawle Valided"
  $objCCDC.Add($objCCD3) | Out-Null

  [System.Diagnostics.PerformanceCounterCategory]::Create($categoryName, $categoryHelp, $categoryType, $objCCDC) | Out-Null
}