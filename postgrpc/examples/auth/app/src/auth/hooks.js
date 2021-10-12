import { toast } from 'react-toastify'
import { useEffect, useState } from 'react'
import { useHistory } from 'react-router-dom'
import {
  logOut,
  getSession,
  startLogin,
  startRegistration,
  completeLogin,
  completeRegistration
} from './requests'

// convert Kratos error responses to readable messages
const toMessage = (action, error) =>
  `${action} failed with status ${error?.status || 'Unknown'}: ${error?.reason || error?.message || 'Bad response from server'}`

// set up silly "caches" for hooks
let CACHED_LOGIN_FLOW = null
let CACHED_REGISTRATION_FLOW = null

// use the Kratos identity or redirect to the login page
export const useIdentity = () => {
  const history = useHistory()
  const [identity, setIdentity] = useState(null)

  // check the session once per render
  useEffect(() => {
    const validateSession = async () => {
      const response = await getSession()

      if (response.error?.code === 401) {
        history.replace('/auth/login')
      } else if (response.error) {
        toast.error(toMessage('Session check', response.error))
      }

      setIdentity(response.identity)
    }

    validateSession()
  }, [history])

  // handle user logout
  const onLogOut = async () => {
    const response = await logOut()

    if (response.ok) {
      // force a new login flow after logout
      CACHED_LOGIN_FLOW = null
      history.replace('/auth/login')
    } else {
      const { error } = await response.json()
      toast.error(toMessage('Logout', error))
    }
  }

  return [identity, onLogOut]
}

// use a wrapper of the entire Kratos login flow
export const useLoginFlow = () => {
  const history = useHistory()
  const [flow, setFlow] = useState(CACHED_LOGIN_FLOW)

  // create a new login flow once on render
  useEffect(() => {
    const createLoginFlow = async () => {
      const sessionResponse = await getSession()

      // redirect active sessions
      if (sessionResponse.active) {
        history.replace('/')
      } else {
        const response = await startLogin()

        if (response?.ui) {
          CACHED_LOGIN_FLOW = response.ui
          setFlow(response.ui)
        } else {
          toast.error(toMessage('Login', response?.error))
        }
      }
    }

    if (!flow) {
      createLoginFlow()
    }
  }, [flow, history])

  const completeFlow = async (form) => {
    const response = await completeLogin(flow, form)

    if (response.ui) {
      // FIXME: render messages for the user
      toast.warn('Additional action required')
      CACHED_LOGIN_FLOW = response.ui
      setFlow(response.ui)
    } else if (response.session?.active) {
      history.replace('/')
    } else {
      history.replace('/auth/login')
    }
  }

  return [flow, completeFlow]
}

// use a wrapper of the entire Kratos login flow
export const useRegistrationFlow = () => {
  const history = useHistory()
  const [flow, setFlow] = useState(CACHED_REGISTRATION_FLOW)

  // create a new registration flow once on render
  useEffect(() => {
    const createRegistrationFlow = async () => {
      const sessionResponse = await getSession()

      // redirect active sessions
      if (sessionResponse.active) {
        history.replace('/')
      } else {
        const response = await startRegistration()

        if (response?.ui) {
          CACHED_REGISTRATION_FLOW = response.ui
          setFlow(response.ui)
        } else {
          toast.error(toMessage('Registration', response?.error))
        }
      }
    }

    if (!flow) {
      createRegistrationFlow()
    }
  }, [flow, history])

  const completeFlow = async (form) => {
    const response = await completeRegistration(flow, form)

    if (response.ui) {
      // FIXME: render messages for the user
      toast.warn('Additional action required')
      CACHED_LOGIN_FLOW = response.ui
      setFlow(response.ui)
    } else if (response.session?.active) {
      history.replace('/')
    } else {
      history.replace('/auth/login')
    }
  }

  return [flow, completeFlow]
}
