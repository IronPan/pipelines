// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by go-swagger; DO NOT EDIT.

package experiment_model

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// APIPredicate Predicate captures individual conditions that must be true for a resource
// being filtered.
// swagger:model apiPredicate
type APIPredicate struct {

	// int value
	IntValue int32 `json:"int_value,omitempty"`

	// Array values below are only meant to be used by the IN operator.
	IntValues *APIIntValues `json:"int_values,omitempty"`

	// key
	Key string `json:"key,omitempty"`

	// long value
	LongValue string `json:"long_value,omitempty"`

	// long values
	LongValues *APILongValues `json:"long_values,omitempty"`

	// op
	Op PredicateOp `json:"op,omitempty"`

	// string value
	StringValue string `json:"string_value,omitempty"`

	// string values
	StringValues *APIStringValues `json:"string_values,omitempty"`

	// Timestamp values will be converted to Unix time (seconds since the epoch)
	// prior to being used in a filtering operation.
	// Format: date-time
	TimestampValue strfmt.DateTime `json:"timestamp_value,omitempty"`
}

// Validate validates this api predicate
func (m *APIPredicate) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateIntValues(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateLongValues(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateOp(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStringValues(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTimestampValue(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *APIPredicate) validateIntValues(formats strfmt.Registry) error {

	if swag.IsZero(m.IntValues) { // not required
		return nil
	}

	if m.IntValues != nil {
		if err := m.IntValues.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("int_values")
			}
			return err
		}
	}

	return nil
}

func (m *APIPredicate) validateLongValues(formats strfmt.Registry) error {

	if swag.IsZero(m.LongValues) { // not required
		return nil
	}

	if m.LongValues != nil {
		if err := m.LongValues.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("long_values")
			}
			return err
		}
	}

	return nil
}

func (m *APIPredicate) validateOp(formats strfmt.Registry) error {

	if swag.IsZero(m.Op) { // not required
		return nil
	}

	if err := m.Op.Validate(formats); err != nil {
		if ve, ok := err.(*errors.Validation); ok {
			return ve.ValidateName("op")
		}
		return err
	}

	return nil
}

func (m *APIPredicate) validateStringValues(formats strfmt.Registry) error {

	if swag.IsZero(m.StringValues) { // not required
		return nil
	}

	if m.StringValues != nil {
		if err := m.StringValues.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("string_values")
			}
			return err
		}
	}

	return nil
}

func (m *APIPredicate) validateTimestampValue(formats strfmt.Registry) error {

	if swag.IsZero(m.TimestampValue) { // not required
		return nil
	}

	if err := validate.FormatOf("timestamp_value", "body", "date-time", m.TimestampValue.String(), formats); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *APIPredicate) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *APIPredicate) UnmarshalBinary(b []byte) error {
	var res APIPredicate
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
