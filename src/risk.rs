use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct RiskManager {
    max_exposure: Decimal,
}

impl RiskManager {
    pub fn new(max_exposure: Decimal) -> Self {
        Self { max_exposure }
    }

    pub fn budget_limit(&self, usdc_balance: Decimal) -> Decimal {
        if usdc_balance < self.max_exposure {
            usdc_balance
        } else {
            self.max_exposure
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBudget {
    limit: Decimal,
    used: Decimal,
}

impl OrderBudget {
    pub fn new(limit: Decimal) -> Self {
        Self { limit, used: Decimal::ZERO }
    }

    pub fn remaining(&self) -> Decimal {
        self.limit - self.used
    }

    pub fn try_reserve(&mut self, notional: Decimal) -> bool {
        if notional <= Decimal::ZERO {
            return false;
        }
        if self.used + notional > self.limit {
            return false;
        }
        self.used += notional;
        true
    }
}
