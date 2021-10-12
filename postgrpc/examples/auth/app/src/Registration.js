import { ClipLoader } from 'react-spinners'
import { Link } from 'react-router-dom'
import { toFields, useRegistrationFlow } from './auth'

const Registration = () => {
  const [flow, completeFlow] = useRegistrationFlow()

  // create an onSubmit handler based on the flow
  const onSubmit = async event => {
    event.preventDefault()
    completeFlow(new FormData(event.target))
  }

  return flow
    ? <>
      <h1>Register</h1>
      <form onSubmit={onSubmit}>
        {toFields(flow)}
      </form>
      <hr />
      <Link to='/auth/login'>Sign in</Link>
    </>
    : <ClipLoader color='white' />
}

export default Registration
