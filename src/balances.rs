use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy)]
pub struct ActualBalances {
    pub up: Decimal,
    pub down: Decimal,
    pub usdc: Decimal,
}

#[derive(Debug, Clone, Copy)]
pub struct PendingBalances {
    pub up: Decimal,
    pub down: Decimal,
}

#[derive(Debug, Clone, Copy)]
pub struct EffectiveBalances {
    pub up: Decimal,
    pub down: Decimal,
}

impl ActualBalances {
    pub fn with_pending(self, pending: PendingBalances) -> EffectiveBalances {
        EffectiveBalances {
            up: self.up + pending.up,
            down: self.down + pending.down,
        }
    }
}
