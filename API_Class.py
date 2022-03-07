
class ProcessOrder:
    def __init__(self, records):
		self.records = records

    def parse_all_orders(self):
             return [self.parse_order(self.record) for self.record in self.records]
    def parse_single_order_line_v1(self):

        def find_in_extended_attribute(attribute_name):
            return next(entry["Value"] for entry in self.order_line["ExtendedAttributes"] if self.entry["Name"] == attribute_name)

            return {
                "Qty": order_line["Quantity"],
                "SKU": order_line["ItemIdentifier"]["SupplierSKU"],
                "UPC": order_line["ItemIdentifier"]["UPC"],
                "Price": order_line["Price"],
                "Cost": order_line["Cost"],
                "MSRP": order_line["MSRP"],
                "Weight": order_line["Weight"],
                "IsDropShip": order_line["IsDropShip"],
                "RetailPrice": order_line["RetailPrice"],
                "ProductName": order_line["Description"],
                "ProductCategory": find_in_extended_attribute("Category"),
                "Engraved": find_in_extended_attribute("Engraved"),
                "Vendor": find_in_extended_attribute("Vendor"),
                "VariantTitle": find_in_extended_attribute("VariantTitle"),
                "ExtendedCategory": find_in_extended_attribute("Category"),
                "AlcoholCategory": find_in_extended_attribute("AlcoholCategory"),
                "IsAlcohol": find_in_extended_attribute("IsAlcohol"),
                "HandmadeLeatherBottleHolder": find_in_extended_attribute("HandmadeLeatherBottleHolder"),
                "LuxuryDrawstring": find_in_extended_attribute("LuxuryDrawstring"),
                "PartnerLineID": find_in_extended_attribute("PartnerLineID"),
                "ExtendedDescription": find_in_extended_attribute("OrderDescription"),
                "LineNumber": order_line["LineNumber"]
            }

    def parse_single_order_line_v2(self):
        """
        Converts a single order line to a dict containing the relevant fields.
        This implementation is more efficient when extended attributes is not large
        and the number of queried attributes is a relatively high percentage.
        """
        extended_attributes = {self.attribute["Name"]: self.attribute["Value"] for self.attribute in self.order_line["ExtendedAttributes"]}
        return {
            "Qty": self.order_line["Quantity"],
            "SKU": self.order_line["ItemIdentifier"]["SupplierSKU"],
            "UPC": self.order_line["ItemIdentifier"]["UPC"],
            "Price": self.order_line["Price"],
            "Cost": self.order_line["Cost"],
            "MSRP": self.order_line["MSRP"],
            "Weight": self.order_line["Weight"],
            "IsDropShip": self.order_line["IsDropShip"],
            "RetailPrice": self.order_line["RetailPrice"],
            "ProductName": self.order_line["Description"],
            "ProductCategory": self.extended_attributes["Category"],
            "Engraved": self.extended_attributes["Engraved"],
            "Vendor": self.extended_attributes["Vendor"],
            "VariantTitle": self.extended_attributes["VariantTitle"],
            "ExtendedCategory": self.extself.ended_attributes["Category"],
            "AlcoholCategory": self.extended_attributes["AlcoholCategory"],
            "IsAlcohol": self.extended_attributes["IsAlcohol"],
            "HandmadeLeatherBottleHolder": self.extended_attributes["HandmadeLeatherBottleHolder"],
            "LuxuryDrawstring": self.extended_attributes["LuxuryDrawstring"],
            "PartnerLineID": self.extended_attributes["PartnerLineID"],
            "ExtendedDescription": self.extended_attributes["OrderDescription"],
            "LineNumber": self.order_line["LineNumber"]
        }

    def parse_order(self):
        """
        Converts an order into a dict containing the relevant fields, duplicating
        information that is the same for all order lines.
        """
        order_info = {
            "Order Number": self.order["OrderNumber"],
            "Order Date": self.order["OrderDate"],
            "DocumentDate": self.order["DocumentDate"],
            "Ship Method": self.order["ShipmentInfos"][0]["ServiceLevelDescription"],
            "Ship To State": self.order["ShipToAddress"]["State"],
            "CustomerNumber": self.order["CustomerNumber"],
            "StatusCode": self.order["StatusCode"],
            "PartnerPO": self.order["PartnerPO"],
            "PayInNumberOfDays": self.order["PaymentTerm"]["PayInNumberOfDays"],
            "DiscountInNumberOfDays": self.order["PaymentTerm"]["DiscountInNumberOfDays"],
            "AvailableDiscount": self.order["PaymentTerm"]["AvailableDiscount"],
            "TotalAmount": self.order["TotalAmount"]
        }
        self.order_lines = [self.parse_single_order_line_v1(self.order_line) for self.order_line in self.order["OrderLines"]]
        return [dict(self.order_info, **self.order_line) for self.order_line in self.order_lines]


