import json

import stripe

from src.server.models.billing.billing_utils import BillingUtils
from src.server.models.session.models.base.base_session_model import BaseSessionModel
from src.server.models.user.user_model import UserModel
from src.utils.gembase_utils import GembaseUtils


class BillingSessionModel(BaseSessionModel):

    def __init__(self, session):
        super(BillingSessionModel, self).__init__(session)
        self.__pricing_def: dict | None = None
        self.__discount_def: dict | None = None
        self.__vat_def: list | None = None
        self.__unlocked_modules: list | None = None
        self.__allowed_tags: dict | None = None
        self.__last_billing_details: dict | None = None

    def is_module_locked(self, module_id: int, ignore_free_trial=False):
        if self.session().is_admin():
            return False
        return module_id not in self.get_unlocked_modules(ignore_free_trial=ignore_free_trial)

    def get_unlocked_modules(self, ignore_free_trial=False):
        if self.__unlocked_modules is None:
            self.__unlocked_modules = BillingUtils.get_unlocked_modules(
                conn=self.conn(),
                user_id=self.user_id(),
                ignore_free_trial=ignore_free_trial
            )
        return self.__unlocked_modules

    def get_def(self):
        return {
            "pricing_def": self.get_pricing_def(to_arr=True),
            "discount_def": self.get_discount_def(to_arr=True),
            "vat_def": self.get_vat_def(),
            "last_billing_details": self.get_last_billing_details()
        }

    def get_pricing_def(self, to_arr=False) -> dict | list:
        if self.__pricing_def is None:
            rows = self.conn().select_all("""
            SELECT id, name, price, invoice_name, invoice_desc
              FROM billing.def_modules_pricing
             WHERE hidden = 0
            """)
            self.__pricing_def = {}
            for row in rows:
                self.__pricing_def[row["id"]] = {
                    "id": row["id"],
                    "price": float(row["price"]),
                    "name": row["name"],
                    "invoice_name": row["invoice_name"],
                    "invoice_desc": row["invoice_desc"]
                }

        if to_arr:
            return [self.__pricing_def[k] for k in self.__pricing_def]

        return self.__pricing_def

    def get_discount_def(self, to_arr=False) -> dict | list:
        if self.__discount_def is None:
            rows = self.conn().select_all("""
            SELECT id, name, discount_perc
              FROM billing.def_modules_discount
            """)
            self.__discount_def = {}
            for row in rows:
                self.__discount_def[row["id"]] = {
                    "id": row["id"],
                    "discount_perc": float(row["discount_perc"]),
                    "name": row["name"]
                }

        if to_arr:
            return [self.__discount_def[k] for k in self.__discount_def]

        return self.__discount_def

    def get_vat_def(self) -> list:
        if self.__vat_def is None:
            rows = self.conn().select_all("""
            SELECT d.country_id, d.code_2, d.vat_name, d.vat_abbreviation, d.vat_characters_min,
                   d.vat_characters_max
              FROM app.def_countries d
             ORDER BY d.code_2
            """)
            self.__vat_def = rows
        return self.__vat_def

    def get_last_billing_details(self) -> dict:
        if self.__last_billing_details is None:
            row = self.conn().select_one_or_none("""
            SELECT billing_details 
              FROM billing.requests 
             WHERE user_id = %s 
             ORDER BY id DESC 
             LIMIT 1
            """, [self.user_id()])

            if row is not None and row["billing_details"] is not None:
                self.__last_billing_details = json.loads(row["billing_details"])

        return self.__last_billing_details

    def payment_request(
            self,
            billing_details: dict,
            modules_config: dict,
            test_payment: bool,
            test_live_payment: bool
    ):
        modules_cnt = 0

        is_discount_multi_seats = False
        is_discount_multi_services = False
        is_discount_testimonial = False
        is_discount_player_research = False

        test_payment_val = 0
        if test_payment:
            test_payment_val = 1
        elif test_live_payment:
            test_payment_val = 2

        tax_rate_id = BillingUtils.init_stripe_api(
            test_payment_val=test_payment_val
        )

        for it in modules_config["modules"]:
            modules_cnt += 1
            it["price_year"] = self.get_pricing_def()[it["id"]]["price"] * 12
            it["price_year_orig"] = it["price_year"]
            if modules_config["players_research"] and it["id"] == BillingUtils.BILLING_MODULE_INSIGHT:
                it["price_year"] = 0
                is_discount_player_research = True

            if it["seats"] >= 2 and it["price_year"] > 0:
                is_discount_multi_seats = True

        discounts_percentage_sum = 0

        if is_discount_multi_seats:
            discounts_percentage_sum += self.get_discount_def()[BillingUtils.DISCOUNT_MULTI_SEATS]["discount_perc"]
        if modules_cnt >= 2:
            is_discount_multi_services = True
            discounts_percentage_sum += self.get_discount_def()[BillingUtils.DISCOUNT_MULTI]["discount_perc"]
        if modules_config["testimonial"]:
            is_discount_testimonial = True
            discounts_percentage_sum += self.get_discount_def()[BillingUtils.DISCOUNT_TESTIMONIAL]["discount_perc"]

        for it in modules_config["modules"]:
            price_year = GembaseUtils.round_price(it["price_year"])
            price_year_discount = GembaseUtils.round_price(price_year * (discounts_percentage_sum / 100))
            price_year_billed = price_year - price_year_discount
            it["price_year_discount"] = price_year_discount
            it["price_year_billed"] = price_year_billed

        request_guid = GembaseUtils.get_guid()

        line_items = []

        for it in modules_config["modules"]:

            unit_amount = int(it["price_year_billed"] * 100)

            d = self.get_pricing_def()[it["id"]]

            desc_discount_arr = []

            if it["id"] == BillingUtils.BILLING_MODULE_INSIGHT and is_discount_player_research:
                desc_discount_arr.append("100% for the player research")
            else:
                if is_discount_multi_seats:
                    desc_discount_arr.append("20% for 2+ seats")
                if is_discount_multi_services:
                    desc_discount_arr.append("20% for 2+ services")
                if is_discount_testimonial:
                    desc_discount_arr.append("20% for a testimonial")

            desc_discount = ""
            if len(desc_discount_arr) > 0:

                original_price_txt = ""
                if it["price_year_orig"] > 0:
                    original_price = int(it["price_year_orig"] * it["seats"])
                    original_price_txt = f" (Original price ${GembaseUtils.format_price(original_price)})"

                desc_discount = ". Discounted by " + " + ".join(desc_discount_arr) + original_price_txt

            desc = d["invoice_desc"] + " - " + desc_discount

            if test_live_payment:
                unit_amount = 1000

            price_data = {
                "currency": "eur",
                "product_data": {
                    "description": desc,
                    "metadata": {
                        "id": f"GB__{it['id']}",
                        "seats": it["seats"]
                    },
                    "name": d["invoice_name"]
                },
                "unit_amount": unit_amount,
                "recurring": {
                    "interval": "year",
                    "interval_count": 1
                },
                "tax_behavior": "exclusive"
            }
            line_items.append({
                "price_data": price_data,
                "quantity": it["seats"],
                "tax_rates": [tax_rate_id]
            })

        if is_discount_testimonial:
            line_items.append({
                "price_data": {
                    "currency": "eur",
                    "product_data": {
                        "description": "I agree to provide a testimonial paragraph about Gembase.io",
                        "name": "Testimonial"
                    },
                    "unit_amount": 0,
                    "recurring": {
                        "interval": "year",
                        "interval_count": 1
                    }
                },
                "quantity": 1
            })

        if is_discount_player_research:
            line_items.append({
                "price_data": {
                    "currency": "eur",
                    "product_data": {
                        "description": "I agree to place Gembase.io surveys in our game(s) to understand traits of our players",
                        "name": "Free player research"
                    },
                    "unit_amount": 0,
                    "recurring": {
                        "interval": "year",
                        "interval_count": 1
                    },
                },
                "quantity": 1,
            })

        client_url_root = GembaseUtils.client_url_root()
        checkout_session = stripe.checkout.Session.create(
            client_reference_id=f"gb__{self.user_id()}",
            customer_email=self.session().user().get_email(),
            line_items=line_items,
            mode='subscription',
            metadata={
                "request": request_guid,
                "user_id": str(self.user_id())
            },
            subscription_data={
                "metadata": {
                    "request": request_guid,
                    "user_id": str(self.user_id())
                }
            },
            success_url=f"{client_url_root}/platform/billing?checkoutResult=success&billingRequest={request_guid}",
            cancel_url=f"{client_url_root}/platform/billing?checkoutResult=cancel&billingRequest={request_guid}"
        )

        self.conn().query("""
                INSERT INTO billing.requests (guid, user_id, billing_details, modules_config, 
                stripe_id, test_payment) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """, [request_guid, self.user_id(), json.dumps(billing_details),
                      json.dumps(modules_config), checkout_session.id, test_payment_val])

        return {
            "redirect": checkout_session.url
        }

    def payment_confirm(
            self,
            request_guid: str
    ):
        row = self.conn().select_one_or_none("""
        SELECT stripe_id, 
               stripe_state, 
               test_payment
          FROM billing.requests
         WHERE guid = %s
        """, [request_guid])

        if row is None:
            return {
                "state": "error"
            }

        if row["stripe_state"] != "request":
            return {
                "state": "error"
            }

        BillingUtils.init_stripe_api(
            test_payment_val=row["test_payment"]
        )

        checkout_session = stripe.checkout.Session.retrieve(row["stripe_id"])

        status = checkout_session["status"]
        payment_status = checkout_session["payment_status"]

        gb_state = "request"

        if status == "complete" and payment_status == "paid":
            gb_state = "success"
            subscription_id = checkout_session.subscription
            subscription = stripe.Subscription.retrieve(subscription_id)
            self.conn().query("""
            UPDATE billing.requests 
               SET stripe_subscription_id = %s
             WHERE guid = %s
            """, [subscription_id, request_guid])
            self.__activate_billing_from_payment(
                payment_guid=request_guid,
                expire_t=subscription.current_period_end
            )
        else:
            if status == "expired":
                gb_state = "cancel"

        if gb_state != "request":
            self.conn().query("""
            UPDATE billing.requests 
               SET stripe_state = %s, 
                   stripe_t = NOW()
             WHERE guid = %s
            """, [gb_state, request_guid])

        return {
            "state": gb_state
        }

    def __activate_billing_from_payment(self, payment_guid: str, expire_t: int):

        row_payment = self.conn().select_one("""
        SELECT r.id, 
               r.user_id, 
               r.billing_details, 
               r.modules_config
          FROM billing.requests r
         WHERE r.guid = %s
        """, [payment_guid])

        row_billing = self.conn().select_one_or_none("""
        SELECT 1
          FROM billing.billings 
         WHERE payment_id = %s
        """, [row_payment["id"]])

        assert row_billing is None

        user_id = row_payment["user_id"]
        modules_config = json.loads(row_payment["modules_config"])

        user_model = UserModel(conn=self.conn(), user_id=user_id)

        if not user_model.is_organization():
            user_model.get_organization().create(credits_value=25)

        self.__create(
            organization_id=user_model.get_organization().get_id(),
            user_id=user_id,
            payment_id=row_payment["id"],
            expire_t=expire_t,
            modules=modules_config["modules"]
        )

        return True

    def __create(
            self,
            organization_id: int,
            user_id: int,
            payment_id: int,
            expire_t: int,
            modules: []
    ):
        billing_id = self.conn().insert("""
        INSERT INTO billing.billings (organization_id, user_id, guid, activated_t, 
        expire_t, payment_id) 
        VALUES (%s, %s, uuid(), NOW(), FROM_UNIXTIME(%s), %s)
        """, [organization_id, user_id, expire_t, payment_id])

        for it in modules:
            self.conn().query("""
            INSERT INTO billing.modules (billing_id, module_id, seats)
            VALUES (%s, %s, %s)
            """, [billing_id, it["id"], it["seats"]])

        return billing_id

    def get_billings(self):
        user_id = self.user_id()
        organization_id = self.session().user().get_organization().get_id()
        is_organization_admin = self.session().user().get_organization().is_organization_admin()

        rows_billings = self.conn().select_all("""
        SELECT b.guid as billing_guid, 
               UNIX_TIMESTAMP(b.expire_t) as expire_t,
               IF(b.expire_t < NOW(), 1, 0) as expired
          FROM billing.billings b
         WHERE b.organization_id = %s
        """, [organization_id])

        rows_modules = self.conn().select_all("""
        SELECT b.guid as billing_guid, 
               m.module_id, m.seats
          FROM billing.billings b,
               billing.modules m
         WHERE b.organization_id = %s
           AND m.billing_id = b.id
        """, [organization_id])

        rows_organization_members = self.conn().select_all("""
        SELECT u.guid as user_guid, 
               u.name, 
               u.email, 
               u.position_role, 
               u.position_area, 
               ou.role, 
               ou.active
          FROM app.organization_users ou,
               app.users u
         WHERE ou.user_id = u.id
           AND ou.organization_id = %s
           AND (ou.user_id = %s OR %s = TRUE)
        """, [organization_id, user_id, is_organization_admin])

        rows_licences = self.conn().select_all("""
        SELECT b.guid as billing_guid, 
               oum.module_id, 
               u.guid as user_guid
          FROM billing.billings b,
               app.organization_users_modules oum,
               app.organization_users ou,        
               app.users u
         WHERE b.organization_id = %s
           AND b.organization_id = oum.organization_id
           AND b.id = oum.billing_id
           AND oum.organization_id = ou.organization_id
           AND oum.user_id = ou.user_id
           AND ou.user_id = u.id
           AND (u.id = %s OR %s = TRUE)
        """, [organization_id, user_id, is_organization_admin])

        rows_organization_requests = []
        rows_organization_requests_modules = []

        if is_organization_admin:
            rows_organization_requests = self.conn().select_all("""
            SELECT o.request_guid, 
                   o.email,
                   UNIX_TIMESTAMP(o.created_t) as created_t, 
                   UNIX_TIMESTAMP(o.sent_request_t) as sent_request_t
              FROM app.organization_requests o
             WHERE o.organization_id = %s
            """, [organization_id])

            rows_organization_requests_modules = self.conn().select_all("""
            SELECT b.guid as billing_guid,
                   r.request_guid,
                   orm.module_id
              FROM app.organization_requests_modules orm,
                   app.organization_requests r,
                   billing.billings b
             WHERE orm.request_id = r.request_id
               AND orm.billing_id = b.id
               AND r.organization_id = %s
            """, [organization_id])

        rows_organization_domains = self.conn().select_all("""
        SELECT od.domain
          FROM app.organization_domains od
         WHERE od.organization_id = %s
        """, [organization_id])

        organization_domains = []
        for row in rows_organization_domains:
            organization_domains.append(row["domain"])

        return {
            "billings": rows_billings,
            "modules": rows_modules,
            "organization_members": rows_organization_members,
            "licences": rows_licences,
            "organization_requests": rows_organization_requests,
            "organization_requests_modules": rows_organization_requests_modules,
            "organization_domains": organization_domains
        }

    def get_allowed_tags(
            self,
            module_id: int
    ):
        if self.__allowed_tags is None:
            self.__allowed_tags = {}
            rows = self.conn().select_all("""
            SELECT d.module_id, d.tag_id_int, d.is_changeable, d.is_loved, d.is_set
              FROM app.def_allowed_tags_per_locked_module d
            """)
            for row in rows:
                if row["module_id"] not in self.__allowed_tags:
                    self.__allowed_tags[row["module_id"]] = []
                self.__allowed_tags[row["module_id"]].append(row)
        if module_id in self.__allowed_tags:
            return self.__allowed_tags[module_id]

        return None
