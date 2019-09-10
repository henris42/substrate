// Copyright 2017-2019 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! Tests for the im-online module.

#![cfg(test)]

use super::*;
use crate::mock::{
	Offences, System, Offence, TestEvent, KIND, new_test_ext, with_on_offence_fractions,
	offence_reports,
};
use system::{EventRecord, Phase};
use runtime_io::with_externalities;

#[test]
fn should_report_an_authority_and_trigger_on_offence() {
	with_externalities(&mut new_test_ext(), || {
		// given
		let time_slot = 42;
		assert_eq!(offence_reports(KIND, time_slot), vec![]);

		let offence = Offence {
			validator_set_count: 5,
			time_slot,
			offenders: vec![5],
		};

		// when
		Offences::report_offence(vec![], offence);

		// then
		with_on_offence_fractions(|f| {
			assert_eq!(f.clone(), vec![Perbill::from_percent(25)]);
		});
	});
}
