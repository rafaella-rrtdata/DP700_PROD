[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "products_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ProductID", DestinationColumnName = "ProductID"], [SourceColumnName = "ProductName", DestinationColumnName = "ProductName"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "ListPrice", DestinationColumnName = "ListPrice"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared products = let
  Origen = Csv.Document(Web.Contents("https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/products.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars = true]),
  #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {{"ProductID", Int64.Type}, {"ProductName", type text}, {"Category", type text}, {"ListPrice", Int64.Type}}, "es")
in
  #"Tipo de columna cambiado";
shared products_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "d8fa42bd-13fe-4f2b-b65e-77a099361a86"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e61b1375-e39d-4959-899b-e03206656f93"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "products", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
