ссылка на папку: https://drive.google.com/file/d/1TPwkup0eItDup6defmTPl1fvRSCB848i/view?usp=sharing 

1) Реестр участников - "\qbs_gd\v3SubjectAll.parquet" - API - https://goszakup.gov.kz/ru/developer/ows_v3#reestr-uchastnikov-reestr-uchastnikov-polnyi-spisok
2) Реестр недобросовестных поставщиков - "\qbs_gd\v3Rnu.parquet" - API - https://goszakup.gov.kz/ru/developer/ows_v3#reestr-nedobrosovestnykh-postavshchikov-reestr-nedobrosovestnykh-postavshchikov
3) Реестр заказчиков - "\qbs_gd\v3Plans.parquet" - API - https://goszakup.gov.kz/ru/developer/ows_v3#reestr-godovykh-planov-reestr-zakazchikov
4) Реестр мест поставки - "\qbs_gd\PlnPointsKato.parquet" - GraphQL - https://ows.goszakup.gov.kz/help/v3/schema/plnpointskato.doc.html
5) Реестр специфик - "\qbs_gd\PlnPointsSpec.parquet" - GraphQL - https://ows.goszakup.gov.kz/help/v3/schema/plnpointsspec.doc.html
6) Список снятых с публикаций пунктов плана - "\qbs_gd\v2PlansDeleted.parquet" - API - https://goszakup.gov.kz/ru/developer/ows_v3#reestr-godovykh-planov-spisok-sniatykh-s-publikatsii-punktov-plana
7) Получение полного списка объявлений - "\qbs_gd\TrdBuy.parquet" - GraphQL - https://ows.goszakup.gov.kz/help/v3/schema/trdbuy.doc.html
8) Получение списка заявок поставщиков - GraphQL
	8.1) Получение списка заявок поставщиков - 
	"\qbs_gd\TrdApp\TrdApp" - 	https://ows.goszakup.gov.kz/help/v3/schema/trdapp.doc.html
	8.2) У TrdApp вложенный Список лотов, прикрепленных к заявке поставщика - 
	"\qbs_gd\TrdApp\TrdAppLots.parquet" - 	https://ows.goszakup.gov.kz/help/v3/schema/trdapplots.doc.html
	8.3) А у TrdAppLots вложенный Ценовые предложения поставщиков по пунктам плана лота - 	"\qbs_gd\TrdApp\TrdAppPriceOfferPoint.parquet" - 	https://ows.goszakup.gov.kz/help/v3/schema/trdpriceofferpoint.doc.html
9) Реестр лотов - "\qbs_gd\Lots.parquet" - GraphQL -https://ows.goszakup.gov.kz/help/v3/schema/lots.doc.html
10) Реестр электронных актов - "\qbs_gd\ContractAct.parquet" - GraphQL - https://ows.goszakup.gov.kz/help/v3/schema/contractact.doc.html
11) Получение полного списка платежей - "\qbs_gd\TreasuryPay.parquet" - GraphQL - https://ows.goszakup.gov.kz/help/v3/schema/treasurypay.doc.html
