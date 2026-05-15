let

  // 1. Fonte de dados
  Fonte = Csv.Document(
    File.Contents("Caminho do arquivo"),
    [Delimiter=",", Columns=19, Encoding=1252, QuoteStyle=QuoteStyle.None]
  ),

  // 2. Promoção de cabeçalhos
  CabecalhosPromovidos = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true]),

  // 3. Ajuste de tipos de dados
  TiposAlterados = Table.TransformColumnTypes(CabecalhosPromovidos,{
    {"VendorID", Int64.Type},
    {"tpep_pickup_datetime", type datetime},
    {"tpep_dropoff_datetime", type datetime},
    {"passenger_count", Int64.Type},
    {"trip_distance", type text},
    {"pickup_longitude", Int64.Type},
    {"pickup_latitude", Int64.Type},
    {"RatecodeID", Int64.Type},
    {"store_and_fwd_flag", type text},
    {"dropoff_longitude", Int64.Type},
    {"dropoff_latitude", Int64.Type},
    {"payment_type", Int64.Type},
    {"fare_amount", type number},
    {"extra", type number},
    {"mta_tax", type number},
    {"tip_amount", type number},
    {"tolls_amount", type number},
    {"improvement_surcharge", type number},
    {"total_amount", type number}
  }),

  // 4. Conversão de texto para número
  SubstituirSeparador = Table.ReplaceValue(TiposAlterados,".","," ,Replacer.ReplaceText,{"trip_distance"}),
  DistanciaNumero = Table.TransformColumnTypes(SubstituirSeparador,{{"trip_distance", type number}}),

  // 5. Filtro de pagamento
  FiltroPagamento = Table.SelectRows(DistanciaNumero, each [payment_type] = 1 or [payment_type] = 2),

  // 6. Duração
  DuracaoMinutos = Table.AddColumn(
    FiltroPagamento, "duration_min",
    each Duration.TotalMinutes([tpep_dropoff_datetime] - [tpep_pickup_datetime]),
    type number
  ),

  // 7. Filtro duração
  FiltroDuracao = Table.SelectRows(DuracaoMinutos, each [duration_min] >= 1 and [duration_min] <= 180),

  // 8. Distância KM
  DistanciaKM = Table.AddColumn(
    FiltroDuracao, "trip_distance_km",
    each Number.Round([trip_distance] * 1.60934, 2),
    type number
  ),

  // 9. Velocidade média
  VelocidadeMedia = Table.AddColumn(
    DistanciaKM, "avg_speed_kmh",
    each [trip_distance_km] / ([duration_min] / 60),
    type number
  ),

  // 10. Filtro velocidade
  FiltroVelocidade = Table.SelectRows(VelocidadeMedia, each [avg_speed_kmh] > 0 and [avg_speed_kmh] <= 120),

  // 11. Receita líquida
  ReceitaLiquida = Table.AddColumn(
    FiltroVelocidade, "receita_liquida",
    each [total_amount] - [tolls_amount] - [tip_amount],
    type number
  ),

  // 12. Classificação valor
  ClassificacaoValor = Table.AddColumn(
    ReceitaLiquida, "trip_value_category",
    each
      if [fare_amount] < 10 then "Baixa"
      else if [fare_amount] <= 30 then "Media"
      else "Alta"
  ),



  // 13. Classificação distância
  ClassificacaoDistancia = Table.AddColumn(
    ClassificacaoValor, "trip_km_category",
    each
      if [trip_distance_km] < 5 then "Curta"
      else if [trip_distance_km] <= 20 then "Media"
      else "Longa"
  ),

in ClassificacaoDistancia


//O segundo conjunto de transformações foi estruturado com o objetivo de aumentar a carga computacional, incluindo operações de ordenação, agregação e cálculos iterativos, permitindo avaliar o impacto do custo de processamento no desempenho das ferramentas analisada

  // 14. Ordenação (peso memória)

  Ordenado = Table.Sort(ClassificacaoDistancia, {{"total_amount", Order.Descending}}),

  // 15. Agrupammento (peso agregação)
  Agrupado = Table.Group(
    Ordenado,
    {"payment_type", "trip_km_category"},
    {
        {"Total Corridas", each Table.RowCount(_), Int64.Type},
        {"Receita Total", each List.Sum([total_amount]), type number},
        {"Velocidade Média", each List.Average([avg_speed_kmh]), type number}
    }
  ),

  // 16. Coluna Para exigir (peso CPU)
  ColunaPesada = Table.AddColumn(
    Agrupado,
    "Calculo_Pesado",
    each List.Sum(
        List.Transform({1..500}, (x) => Number.Power([Receita Total] + x, 2))
    ),
    type number
  )

in
  ColunaPesada

