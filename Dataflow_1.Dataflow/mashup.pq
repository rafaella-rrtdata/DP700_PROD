[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "orders1_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "SalesOrderID", DestinationColumnName = "SalesOrderID"], [SourceColumnName = "OrderDate", DestinationColumnName = "OrderDate"], [SourceColumnName = "CustomerID", DestinationColumnName = "CustomerID"], [SourceColumnName = "LineItem", DestinationColumnName = "LineItem"], [SourceColumnName = "ProductID", DestinationColumnName = "ProductID"], [SourceColumnName = "OrderQty", DestinationColumnName = "OrderQty"], [SourceColumnName = "LineItemTotal", DestinationColumnName = "LineItemTotal"], [SourceColumnName = "MontNo", DestinationColumnName = "MontNo"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared orders1 = let
  Origen = Csv.Document(Web.Contents("https://rrtdata-my.sharepoint.com/personal/rafaellaromer_rrtdata_onmicrosoft_com/Documents/Aplicaciones/Microsoft Power Query/Uploaded Files/orders 1.csv"), [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars = true]),
  #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {{"SalesOrderID", Int64.Type}, {"OrderDate", type date}, {"CustomerID", Int64.Type}, {"LineItem", Int64.Type}, {"ProductID", Int64.Type}, {"OrderQty", Int64.Type}, {"LineItemTotal", Int64.Type}}, "es"),
  #"Personalizado agregado" = Table.TransformColumnTypes(Table.AddColumn(#"Tipo de columna cambiado", "MontNo", each Date.Month([OrderDate])), {{"MontNo", Int64.Type}})
in
  #"Personalizado agregado";
shared DefaultDestination = let
  Origen = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navegación 1" = Origen{[workspaceId = "4556cc13-7701-4e97-9168-95ccce590897"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "5cfa9d05-eec7-48a5-91ae-74004b1f55e5"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "dbo", ItemKind = "Schema"]}[Data]
in
  #"Navegación 3";
shared orders1_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "4556cc13-7701-4e97-9168-95ccce590897"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5cfa9d05-eec7-48a5-91ae-74004b1f55e5"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "orders", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
