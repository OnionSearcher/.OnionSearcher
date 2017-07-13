$categoryName = "WebSearcherManager"
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
  $objCCD4.CounterName = "Pages"
  $objCCD4.CounterType = "NumberOfItems32"
  $objCCD4.CounterHelp = "Web Searcher Pages"
  $objCCDC.Add($objCCD4) | Out-Null
 
  $objCCD7 = New-Object System.Diagnostics.CounterCreationData
  $objCCD7.CounterName = "PagesOk"
  $objCCD7.CounterType = "NumberOfItems32"
  $objCCD7.CounterHelp = "Web Searcher Pages Up"
  $objCCDC.Add($objCCD7) | Out-Null

  $objCCD5 = New-Object System.Diagnostics.CounterCreationData
  $objCCD5.CounterName = "HiddenServices"
  $objCCD5.CounterType = "NumberOfItems32"
  $objCCD5.CounterHelp = "Web Searcher Hidden Services"
  $objCCDC.Add($objCCD5) | Out-Null
	
  $objCCD6 = New-Object System.Diagnostics.CounterCreationData
  $objCCD6.CounterName = "HiddenServicesOk"
  $objCCD6.CounterType = "NumberOfItems32"
  $objCCD6.CounterHelp = "Web Searcher Hidden Services Up"
  $objCCDC.Add($objCCD6) | Out-Null

  [System.Diagnostics.PerformanceCounterCategory]::Create($categoryName, $categoryHelp, $categoryType, $objCCDC) | Out-Null
}
