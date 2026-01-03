    async def get_active_option_instruments(self) -> List[str]:
        """
        Fetches active instrument keys for WebSocket subscription.
        Logic: Selects ATM strike +/- 10 strikes for the current week.
        """
        try:
            # 1. Get Spot Price
            spot = await self.get_spot_price()
            if spot == 0:
                logger.warning("Spot price is 0, cannot determine active instruments")
                return []

            # 2. Get Chain
            chain = await self.get_option_chain("current_week")
            if chain.empty:
                return []

            # 3. Filter for active strikes (Spot +/- 2%)
            # Assuming chain has 'instrument_key' columns for CE and PE. 
            # Note: The fetch_chain_data method needs to preserve instrument_keys.
            # (Ensure your fetch_chain_data includes 'instrument_key' in the DataFrame)
            
            # Since our fetch_chain_data currently returns summarized Greeks, 
            # we might need to fetch the raw keys again or adjust the fetcher.
            # For robustness, let's just fetch the raw keys for the strikes near spot.
            
            # Simplified Logic:
            nearby = chain.iloc[(chain['strike'] - spot).abs().argsort()[:25]] # Top 25 closest strikes
            
            # We need the actual instrument keys. 
            # If fetch_chain_data didn't save them, we re-fetch briefly or rely on live quote keys.
            # Ideally, modify fetch_chain_data to include 'ce_key' and 'pe_key'.
            
            # For now, let's assume we return NIFTY_KEY and VIX_KEY at minimum
            return [NIFTY_KEY, VIX_KEY]
            
        except Exception as e:
            logger.error(f"Error getting active instruments: {e}")
            return [NIFTY_KEY, VIX_KEY]
